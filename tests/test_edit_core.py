import unittest
from unittest.mock import Mock

import pandas as pd

from shopify_sync import edit_core


class EditCoreProductStatusTests(unittest.TestCase):
    OWNER_ID = "gid://shopify/Product/123"

    @staticmethod
    def _rows(*rows):
        return pd.DataFrame(
            [
                {
                    "_skip_reason": "",
                    "row_type": "core",
                    "sheet_row": index + 2,
                    "entity_type": "PRODUCT",
                    "owner_id": EditCoreProductStatusTests.OWNER_ID,
                    **row,
                }
                for index, row in enumerate(rows)
            ]
        )

    def test_module_version_is_product_status_v10(self):
        self.assertEqual(
            edit_core.MODULE_VERSION,
            "edit_core_product_status_v10_20260730",
        )

    def test_status_is_product_only_and_set_only(self):
        self.assertEqual(
            edit_core.validate_row("PRODUCT", "core.status", "SET"),
            (True, ""),
        )
        self.assertEqual(
            edit_core.validate_row("VARIANT", "core.status", "SET"),
            (False, "core_entity_mismatch"),
        )
        self.assertEqual(
            edit_core.validate_row("PRODUCT", "core.status", "CLEAR"),
            (False, "action_not_supported"),
        )

    def test_status_values_are_normalized_into_product_inputs_and_preview(self):
        for desired, expected in (
            (" active ", "ACTIVE"),
            ("draft", "DRAFT"),
            ("ARCHIVED", "ARCHIVED"),
        ):
            with self.subTest(desired=desired):
                plan = edit_core.build_core_plan(
                    self._rows(
                        {
                            "field_key": "core.status",
                            "action": "SET",
                            "desired_value": desired,
                        }
                    ),
                    Mock(),
                )

                self.assertEqual(
                    plan["product_inputs"],
                    [{"id": self.OWNER_ID, "status": expected}],
                )
                self.assertEqual(plan["preview_rows"][0]["value_preview"], expected)
                self.assertEqual(plan["invalid_rows"], [])

    def test_blank_and_unsupported_status_values_fail_plan_validation(self):
        for desired in ("", "   ", "PUBLISHED"):
            with self.subTest(desired=desired):
                plan = edit_core.build_core_plan(
                    self._rows(
                        {
                            "field_key": "core.status",
                            "action": "SET",
                            "desired_value": desired,
                        }
                    ),
                    Mock(),
                )

                self.assertEqual(plan["product_inputs"], [])
                self.assertEqual(plan["preview_rows"], [])
                self.assertEqual(len(plan["invalid_rows"]), 1)
                self.assertEqual(
                    plan["invalid_rows"][0]["error_reason"],
                    "invalid_core_value",
                )

    def test_status_is_additive_to_existing_product_core_fields(self):
        plan = edit_core.build_core_plan(
            self._rows(
                {
                    "field_key": "core.title",
                    "action": "SET",
                    "desired_value": "Existing title behavior",
                },
                {
                    "field_key": "core.status",
                    "action": "SET",
                    "desired_value": "active",
                },
            ),
            Mock(),
        )

        self.assertEqual(
            plan["product_inputs"],
            [
                {
                    "id": self.OWNER_ID,
                    "title": "Existing title behavior",
                    "status": "ACTIVE",
                }
            ],
        )
        self.assertEqual(
            [row["field_key"] for row in plan["preview_rows"]],
            ["core.title", "core.status"],
        )
        self.assertEqual(plan["invalid_rows"], [])


if __name__ == "__main__":
    unittest.main()
