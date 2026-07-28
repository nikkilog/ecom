import unittest

from shopify_create import generic_prepare as prepare


class GenericPrepareTests(unittest.TestCase):
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

    def test_draft_all_channels_is_warning_not_error(self):
        field_keys = [
            "sys.action",
            "sys.product_key",
            "sys.variant_key",
            "core.title",
            "core.sku",
            "core.price",
            "core.status",
            "publish.all_channels",
        ]
        input_contract = {
            "field_keys": field_keys,
            "columns": [
                {"field_key": key, "display_name": key}
                for key in field_keys
            ],
            "rows": [
                {
                    "source_row": 3,
                    "values": {
                        "sys.action": "CREATE",
                        "sys.product_key": "20101000010",
                        "sys.variant_key": "20101000010",
                        "core.title": "Pallet Jack Part",
                        "core.sku": "20101000010",
                        "core.price": "10.00",
                        "core.status": "draft",
                        "publish.all_channels": "TRUE",
                    },
                }
            ],
        }

        plan = prepare._build_prepare_plan(
            input_contract=input_contract,
            defaults={},
            cfg_fields={},
            locations={"active_by_code": {}, "default": {}},
        )

        self.assertEqual(plan["status"], "READY_WITH_WARNINGS")
        self.assertTrue(plan["ready_for_apply"])
        self.assertEqual(plan["stats"]["error_count"], 0)
        self.assertEqual(plan["stats"]["warning_count"], 1)
        self.assertEqual(
            plan["warnings"][0]["code"],
            "DRAFT_WITH_ALL_CHANNEL_PUBLICATION_INTENT",
        )


if __name__ == "__main__":
    unittest.main()
