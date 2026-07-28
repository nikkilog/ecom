import ast
import inspect
import textwrap
import unittest
from unittest import mock

from shopify_create import generic_apply as apply
from shopify_create import generic_prepare as prepare


class _PreflightClient:
    def __init__(self, products_by_handle):
        self.products_by_handle = products_by_handle
        self.calls = []

    def gql(self, query, variables, *, operation_name):
        self.calls.append(
            {
                "query": query,
                "variables": variables,
                "operation_name": operation_name,
            }
        )
        handle = variables["identifier"]["handle"]
        return {"product": self.products_by_handle.get(handle)}


class GenericApplyTests(unittest.TestCase):
    def test_apply_prepare_version_contract(self):
        self.assertEqual(apply.MODULE_VERSION, "1.5.2")
        self.assertEqual(prepare.MODULE_VERSION, "1.6.3")
        self.assertEqual(apply.EXPECTED_PREPARE_MODULE_VERSION, "1.6.3")
        self.assertEqual(
            prepare.MODULE_VERSION,
            apply.EXPECTED_PREPARE_MODULE_VERSION,
        )

    def test_local_shopify_secret_name_is_canonicalized(self):
        with mock.patch.object(
            apply.gp,
            "_runtime_mode",
            return_value="LOCAL",
        ):
            self.assertEqual(
                apply._project_secret_name_for_runtime(
                    "Apollo_SHOPIFY_ACCESS_TOKEN",
                    "apollo",
                ),
                "APOLLO_SHOPIFY_ACCESS_TOKEN",
            )

    def test_handle_preflight_checks_handle_only_and_reports_conflict(self):
        client = _PreflightClient(
            {
                "existing-handle": {
                    "id": "gid://shopify/Product/1",
                    "handle": "existing-handle",
                }
            }
        )
        rows = {
            "p1": [
                {
                    "core.handle": "existing-handle",
                    "core.sku": "duplicate-sku-is-not-identity",
                    "core.barcode": "duplicate-barcode-is-not-identity",
                }
            ],
            "p2": [
                {
                    "core.handle": "new-handle",
                    "core.sku": "duplicate-sku-is-not-identity",
                    "core.barcode": "duplicate-barcode-is-not-identity",
                }
            ],
        }

        result = apply._preflight_shopify_handle_conflicts(
            client=client,
            product_rows=rows,
            progress_every=0,
        )

        self.assertEqual(result["checks"], 2)
        self.assertEqual(result["identity_field"], "core.handle")
        self.assertFalse(result["sku_checked"])
        self.assertFalse(result["barcode_checked"])
        self.assertEqual(
            [item["code"] for item in result["errors"]],
            ["HANDLE_ALREADY_EXISTS"],
        )
        self.assertTrue(
            all(call["operation_name"] == "preflight_handle"
                for call in client.calls)
        )

    def test_shopify_write_calls_are_guarded_by_non_dry_run_branch(self):
        tree = ast.parse(textwrap.dedent(inspect.getsource(apply.run)))
        guarded_write_operations = set()
        unguarded_write_operations = set()

        def operation_name(call):
            for keyword in call.keywords:
                if (
                    keyword.arg == "operation_name"
                    and isinstance(keyword.value, ast.Constant)
                ):
                    return keyword.value.value
            return None

        def collect_calls(nodes):
            operations = set()
            for node in nodes:
                for child in ast.walk(node):
                    if isinstance(child, ast.Call):
                        name = operation_name(child)
                        if name in {
                            "productSet_create",
                            "publishablePublish_all_channels",
                        }:
                            operations.add(name)
            return operations

        for node in ast.walk(tree):
            if not isinstance(node, ast.If):
                continue
            if isinstance(node.test, ast.Name) and node.test.id == "dry_run":
                guarded_write_operations |= collect_calls(node.orelse)
                unguarded_write_operations |= collect_calls(node.body)

        self.assertIn("productSet_create", guarded_write_operations)
        self.assertNotIn("productSet_create", unguarded_write_operations)
        self.assertNotIn(
            "publishablePublish_all_channels",
            unguarded_write_operations,
        )


if __name__ == "__main__":
    unittest.main()
