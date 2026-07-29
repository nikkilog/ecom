import unittest

from shopify_create import generic_prepare as prepare


class GenericPrepareTests(unittest.TestCase):
    FIELD_KEYS = [
        "sys.action",
        "sys.product_key",
        "sys.variant_key",
        "core.title",
        "core.handle",
        "core.sku",
        "core.barcode",
        "core.price",
        "core.status",
        "publish.all_channels",
    ]

    def _plan(self, rows):
        return prepare._build_prepare_plan(
            input_contract={
                "field_keys": self.FIELD_KEYS,
                "columns": [
                    {"field_key": key, "display_name": key}
                    for key in self.FIELD_KEYS
                ],
                "rows": [
                    {"source_row": index + 3, "values": row}
                    for index, row in enumerate(rows)
                ],
            },
            defaults={},
            cfg_fields={},
            locations={"active_by_code": {}, "default": {}},
        )

    @staticmethod
    def _row(product_key, variant_key, handle, **overrides):
        row = {
            "sys.action": "CREATE",
            "sys.product_key": product_key,
            "sys.variant_key": variant_key,
            "core.title": "Product",
            "core.handle": handle,
            "core.sku": variant_key,
            "core.barcode": f"BAR-{variant_key}",
            "core.price": "10.00",
            "core.status": "draft",
            "publish.all_channels": "TRUE",
        }
        row.update(overrides)
        return row

    def test_explicit_entity_type_wins_over_field_prefix(self):
        self.assertEqual(
            prepare._normalize_owner_entity(
                "COLLECTION",
                "mf.custom.breadcrumb_leaf",
            ),
            "COLLECTION",
        )
        self.assertEqual(
            prepare._normalize_owner_entity("", "mf.custom.material"),
            "PRODUCT",
        )
        self.assertEqual(
            prepare._normalize_owner_entity("", "v_mf.custom.material"),
            "VARIANT",
        )

    def test_single_product_single_variant_is_ready(self):
        plan = self._plan([
            self._row("P001", "V001", "product-a"),
        ])

        self.assertEqual(plan["status"], "READY")
        self.assertTrue(plan["ready_for_apply"])
        self.assertEqual(plan["stats"]["product_groups"], 1)
        self.assertEqual(plan["stats"]["variant_objects_planned"], 1)
        self.assertEqual(plan["warnings"], [])
        self.assertEqual(plan["errors"], [])

    def test_same_product_multiple_variants_shared_handle_is_ready(self):
        plan = self._plan([
            self._row("P001", "V001", "product-a"),
            self._row(
                "P001",
                "V002",
                "product-a",
                **{
                    "core.title": "Conflicting title is not checked",
                    "core.sku": "DIFFERENT-SKU",
                    "core.barcode": "DIFFERENT-BARCODE",
                },
            ),
        ])

        self.assertEqual(plan["status"], "READY")
        self.assertEqual(plan["stats"]["product_groups"], 1)
        self.assertEqual(plan["stats"]["variant_objects_planned"], 2)
        self.assertEqual(plan["stats"]["warning_count"], 0)
        self.assertEqual(plan["stats"]["error_count"], 0)
        self.assertTrue(all(
            row["status"] == "READY"
            for row in plan["row_states"]
        ))

    def test_different_product_groups_reusing_handle_are_rejected(self):
        plan = self._plan([
            self._row("P001", "V001", "product-a"),
            self._row("P002", "V002", "product-a"),
        ])

        self.assertEqual(plan["status"], "FAILED_VALIDATION")
        self.assertFalse(plan["ready_for_apply"])
        self.assertEqual(
            {issue["code"] for issue in plan["errors"]},
            {"DUPLICATE_HANDLE"},
        )
        self.assertEqual(plan["stats"]["error_count"], 2)

    def test_cross_product_handle_comparison_trims_and_casefolds(self):
        plan = self._plan([
            self._row("P001", "V001", " Product-A "),
            self._row("P002", "V002", "product-a"),
        ])

        self.assertEqual(plan["status"], "FAILED_VALIDATION")
        self.assertEqual(
            {issue["code"] for issue in plan["errors"]},
            {"DUPLICATE_HANDLE"},
        )

    def test_cancelled_business_checks_do_not_return(self):
        plan = self._plan([
            self._row(
                "P001",
                "DUPLICATE-VARIANT-KEY",
                "product-a",
                **{
                    "core.title": "Title A",
                    "core.sku": "DUPLICATE-SKU",
                    "core.barcode": "DUPLICATE-BARCODE",
                },
            ),
            self._row(
                "P001",
                "DUPLICATE-VARIANT-KEY",
                " PRODUCT-A ",
                **{
                    "core.title": "Title B",
                    "core.sku": "DUPLICATE-SKU",
                    "core.barcode": "DUPLICATE-BARCODE",
                },
            ),
        ])

        self.assertEqual(plan["status"], "READY")
        self.assertEqual(plan["warnings"], [])
        self.assertEqual(plan["errors"], [])
        self.assertEqual(plan["stats"]["warning_count"], 0)
        self.assertEqual(plan["stats"]["error_count"], 0)


if __name__ == "__main__":
    unittest.main()
