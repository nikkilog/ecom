import inspect
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from shopify_create import generic_apply as apply
from shopify_create import generic_prepare as prepare


class _RecordingClient:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    def gql(self, query, variables, *, operation_name):
        self.calls.append(operation_name)
        if self.fail:
            raise RuntimeError("simulated Product failure")
        return {
            "productSet": {
                "product": {
                    "id": "gid://shopify/Product/1",
                    "handle": variables["input"]["handle"],
                    "variants": {
                        "nodes": [{
                            "id": "gid://shopify/ProductVariant/1",
                            "selectedOptions": [],
                            "inventoryItem": {
                                "id": "gid://shopify/InventoryItem/1",
                            },
                        }]
                    },
                },
                "userErrors": [],
            }
        }


class _GoogleWriteError(Exception):
    def __init__(self, status, retry_after=None):
        headers = {}
        if retry_after is not None:
            headers["Retry-After"] = str(retry_after)
        self.response = SimpleNamespace(
            status_code=status,
            headers=headers,
        )
        super().__init__(f"HTTP {status}")


class _FakeWorksheet:
    def __init__(self, *, failures=None, row_count=2, col_count=1):
        self.failures = list(failures or [])
        self.row_count = row_count
        self.col_count = col_count
        self.resize_calls = []
        self.update_calls = []
        self.update_threads = []

    def get_all_values(self):
        return [apply.RESULT_HEADERS]

    def resize(self, *, rows, cols):
        self.resize_calls.append((rows, cols))
        self.row_count = rows
        self.col_count = cols

    def update(self, *, range_name, values, value_input_option):
        self.update_threads.append(threading.current_thread().name)
        if self.failures:
            raise _GoogleWriteError(self.failures.pop(0))
        self.update_calls.append(
            (range_name, values, value_input_option)
        )


def _worker_kwargs(*, duplicate_match=None, dry_run=False, client=None):
    rows = [{
        "sys.source_row": "3",
        "sys.product_key": "P001",
        "sys.variant_key": "V001",
        "core.handle": "product-a",
        "core.title": "Product A",
        "core.sku": "SKU-A",
        "core.barcode": "BAR-A",
    }]
    return {
        "product_key": "product-a",
        "rows": rows,
        "product_input": {
            "title": "Product A",
            "handle": "product-a",
            "status": "DRAFT",
            "variants": [{"optionValues": []}],
        },
        "duplicate_match": duplicate_match,
        "publish_this_product": False,
        "publications": [],
        "dry_run": dry_run,
        "client": client or _RecordingClient(),
        "run_id": "run-1",
        "applied_at": "2026-07-29",
        "site_code": "APOLLO",
        "admin_product_base_url": "",
        "storefront_product_base_url": "",
        "tab_product_handle": "V_Product_Handle",
        "product_handle_header": "Product Handle",
    }


class GenericApplyTests(unittest.TestCase):
    def test_apply_prepare_version_contract(self):
        self.assertEqual(apply.MODULE_VERSION, "1.5.10")
        self.assertEqual(prepare.MODULE_VERSION, "1.6.6")
        self.assertEqual(apply.EXPECTED_PREPARE_MODULE_VERSION, "1.6.6")

    def test_snapshot_uses_named_column_and_normalizes_handles(self):
        snapshot = apply._read_existing_product_handle_snapshot(
            [
                ["SKU", "Product Handle", "Product ID (numeric)"],
                ["SKU-1", " Product-A ", "11"],
                ["SKU-2", "", "12"],
                ["SKU-3", "product-a", "13"],
                ["SKU-4", "PRODUCT-B", "14"],
            ],
            header_name="Product Handle",
            tab_name="V_Product_Handle",
        )

        self.assertEqual(snapshot["rows_loaded"], 4)
        self.assertEqual(snapshot["nonblank_handle_rows"], 3)
        self.assertEqual(snapshot["unique_handles"], 2)
        self.assertEqual(
            set(snapshot["handles_by_normalized"]),
            {"product-a", "product-b"},
        )

    def test_handle_check_uses_handle_only_and_zero_shopify_lookups(self):
        snapshot = apply._read_existing_product_handle_snapshot(
            [["Product Handle"], ["EXISTING"]],
            header_name="Product Handle",
            tab_name="V_Product_Handle",
        )
        result = apply._check_handles_against_snapshot(
            product_rows={
                "existing": [{
                    "core.handle": " existing ",
                    "core.sku": "same",
                    "core.barcode": "same",
                    "sys.product_key": "same",
                    "sys.variant_key": "same",
                    "core.title": "ignored",
                    "product_id": "1",
                    "variant_id": "2",
                }],
                "new": [{
                    "core.handle": "new",
                    "core.sku": "same",
                    "core.barcode": "same",
                    "sys.product_key": "same",
                    "sys.variant_key": "same",
                    "core.title": "ignored",
                    "product_id": "1",
                    "variant_id": "2",
                }],
            },
            snapshot=snapshot,
        )

        self.assertEqual(set(result["duplicates_by_handle"]), {"existing"})
        self.assertEqual(result["queried_fields"], ["core.handle"])
        self.assertEqual(result["shopify_handle_api_requests"], 0)
        for field in (
            "sku_checked",
            "barcode_checked",
            "product_key_checked",
            "variant_key_checked",
            "product_id_checked",
            "variant_id_checked",
        ):
            self.assertFalse(result[field])

    def test_existing_handle_skips_shopify_and_generates_result(self):
        client = _RecordingClient()
        outcome = apply._execute_product_task(**_worker_kwargs(
            duplicate_match={"id": "", "handle": "product-a"},
            client=client,
        ))

        self.assertEqual(outcome["status"], "SKIPPED_HANDLE_EXISTS")
        self.assertEqual(client.calls, [])
        self.assertEqual(outcome["products_failed"], 0)
        self.assertEqual(outcome["products_skipped_handle_exists"], 1)
        self.assertEqual(len(outcome["result_rows"]), 1)
        status_index = apply.RESULT_HEADERS.index("apply_status")
        planned_index = apply.RESULT_HEADERS.index(
            "api_operations_planned"
        )
        self.assertEqual(
            outcome["result_rows"][0][status_index],
            "SKIPPED_HANDLE_EXISTS",
        )
        self.assertEqual(outcome["result_rows"][0][planned_index], 0)

    def test_new_handle_enters_product_worker(self):
        client = _RecordingClient()
        with mock.patch.object(
            apply,
            "_verify_inventory_delivery_fields",
        ):
            outcome = apply._execute_product_task(**_worker_kwargs(
                client=client,
            ))

        self.assertEqual(outcome["status"], "SUCCESS")
        self.assertEqual(client.calls, ["productSet_create"])

    def test_product_worker_has_no_google_sheet_writes(self):
        source = inspect.getsource(apply._execute_product_task)
        self.assertNotIn("worksheet.", source)
        self.assertNotIn("logger.", source)
        self.assertNotIn("_ResultBatchWriter", source)

    def test_google_retry_statuses_and_retry_after(self):
        for status in (429, 500, 502, 503, 504):
            attempts = []

            def operation():
                attempts.append(status)
                if len(attempts) == 1:
                    raise _GoogleWriteError(status, retry_after=7)
                return "ok"

            with mock.patch.object(apply.time, "sleep") as sleep:
                self.assertEqual(
                    apply._google_write_with_retry(
                        operation,
                        action="test",
                        max_retries=2,
                        base_seconds=1,
                        print_progress=False,
                    ),
                    "ok",
                )
            sleep.assert_called_once_with(7.0)

    def test_result_writer_presizes_once_and_retains_rows_during_retry(self):
        worksheet = _FakeWorksheet(
            failures=[429, 503],
            row_count=2,
            col_count=1,
        )
        rows = [["value"] * len(apply.RESULT_HEADERS)]

        with mock.patch.object(apply.time, "sleep"):
            writer = apply._ResultBatchWriter(
                worksheet,
                expected_append_rows=250,
                print_progress=False,
            )
            self.assertEqual(len(worksheet.resize_calls), 1)
            self.assertEqual(writer.next_row, 2)
            written = writer.append(rows)

        self.assertEqual(written, 1)
        self.assertEqual(writer.next_row, 3)
        self.assertEqual(writer.rows_written, 1)
        self.assertEqual(len(worksheet.update_calls), 1)
        self.assertEqual(worksheet.update_calls[0][1], rows)
        self.assertTrue(all(
            name == threading.current_thread().name
            for name in worksheet.update_threads
        ))

    def test_run_orchestration_contract_is_present(self):
        source = inspect.getsource(apply.run)
        self.assertNotIn("_verify_preview_snapshot(", source)
        self.assertNotIn("_preflight_shopify_handle_conflicts", source)
        self.assertEqual(
            source.count(
                "tab_product_handle,\n"
                "        ).get_all_values()"
            ),
            1,
        )
        self.assertIn("ThreadPoolExecutor(", source)
        self.assertIn("max_workers=worker_count", source)
        self.assertIn(
            "if status == \"FAILED\" and stop_on_first_error:",
            source,
        )
        self.assertIn(
            "while not stop_requested and len(pending) < worker_count:",
            source,
        )
        self.assertIn(
            "if result_writer is not None and result_rows_buffer:",
            source,
        )
        self.assertIn('action="write final RunLog"', source)
        self.assertIn('action="write failure RunLog"', source)


if __name__ == "__main__":
    unittest.main()
