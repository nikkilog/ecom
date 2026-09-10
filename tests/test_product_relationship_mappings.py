import importlib
import unittest


mapping = importlib.import_module(
    "shopify_pre_edit.0_4_1_product_relationship_mappings"
)


class ProductRelationshipMappingTests(unittest.TestCase):
    SIZE_ORDER = [
        {"排序": "1", "size": "Small"},
        {"排序": "2", "size": "Medium"},
        {"排序": "3", "size": "Large"},
    ]

    @staticmethod
    def row(product_id, product_type, spu, sku, variant_base, size):
        return {
            "Product ID (numeric)": product_id,
            "Product Type Internal": product_type,
            "SPU-V": spu,
            "SKU-2": sku,
            "Variant Base": variant_base,
            "Size-V": size,
        }

    def build(self, rows):
        return mapping.build_mapping_tables(rows, self.SIZE_ORDER)

    def test_size_order_other_size_and_product_group(self):
        rows = [
            self.row("30", "Each Box", "S1", "SKU-L", "v-l", "Large"),
            self.row("10", "Each Box", "S1", "SKU-S", "v-s", "Small"),
            self.row("20", "Each Box", "S1", "SKU-M", "v-m", "Medium"),
            self.row("110", "Wholesale", "", "", "v-s", ""),
            self.row("120", "Wholesale", "", "", "v-m", ""),
            self.row("130", "Wholesale", "", "", "v-l", ""),
        ]
        result = self.build(rows)
        other = result["tables"][mapping.OUTPUT_TAB_OTHER_E]["rows"]
        group = result["tables"][mapping.OUTPUT_TAB_GROUP_E]["rows"]
        by_pid_other = {row[3]: row[4] for row in other}
        self.assertEqual(by_pid_other["10"], "20,30")
        self.assertEqual(by_pid_other["20"], "10,30")
        self.assertEqual(by_pid_other["30"], "10,20")
        self.assertEqual({row[4] for row in group}, {"10,20,30"})

    def test_w_inherits_e_value_without_wholesale_regrouping(self):
        rows = [
            self.row("10", "Each Box", "S1", "SKU-S", "v-s", "Small"),
            self.row("20", "Each Box", "S1", "SKU-L", "v-l", "Large"),
            self.row("110", "Wholesale", "DIFFERENT", "", "v-s", "Large"),
            self.row("120", "Wholesale", "DIFFERENT", "", "v-l", "Small"),
        ]
        result = self.build(rows)
        other_w = result["tables"][mapping.OUTPUT_TAB_OTHER_W]["rows"]
        group_w = result["tables"][mapping.OUTPUT_TAB_GROUP_W]["rows"]
        self.assertEqual({row[0]: row[1] for row in other_w}, {"110": "20", "120": "10"})
        self.assertEqual({row[1] for row in group_w}, {"10,20"})

    def test_unmapped_size_is_last_and_pid_is_tie_breaker(self):
        rows = [
            self.row("30", "Each Box", "S1", "A", "a", "Unknown"),
            self.row("20", "Each Box", "S1", "B", "b", "Unknown"),
            self.row("10", "Each Box", "S1", "C", "c", "Small"),
        ]
        result = self.build(rows)
        group = result["tables"][mapping.OUTPUT_TAB_GROUP_E]["rows"]
        self.assertEqual({row[4] for row in group}, {"10,20,30"})
        self.assertTrue(any("SIZE_NOT_IN_ORDER" in warning for warning in result["warnings"]))

    def test_multi_size_target_product_is_unresolved_and_warned(self):
        rows = [
            self.row("10", "Each Box", "S1", "A", "a", "Small"),
            self.row("10", "Each Box", "S1", "A2", "a2", "Large"),
            self.row("20", "Each Box", "S1", "B", "b", "Medium"),
        ]
        result = self.build(rows)
        group = result["tables"][mapping.OUTPUT_TAB_GROUP_E]["rows"]
        self.assertEqual({row[4] for row in group}, {"20,10"})
        self.assertTrue(any("PRODUCT_ID_MULTI_SIZE_SOURCE" in warning for warning in result["warnings"]))

    def test_duplicate_size_warns_without_blocking(self):
        rows = [
            self.row("10", "Each Box", "S1", "A", "a", "Small"),
            self.row("20", "Each Box", "S1", "B", "b", "Small"),
        ]
        result = self.build(rows)
        table = result["tables"][mapping.OUTPUT_TAB_OTHER_E]
        self.assertTrue(all(row[6] == mapping.SUCCESS_YES for row in table["rows"]))
        self.assertTrue(all("DUPLICATE_SIZE_IN_SPU" in row[7] for row in table["rows"]))

    def test_filters_pack_and_box_case_insensitively(self):
        rows = [
            self.row("10", "Each Box", "S1", "A-Pack", "a", "Small"),
            self.row("20", "Each Box", "S1", "B-box", "b", "Medium"),
            self.row("30", "Each Box", "S1", "C", "c", "Large"),
        ]
        result = self.build(rows)
        self.assertEqual(result["summary"]["target_rows"], 1)

    def test_wholesale_missing_is_retained_as_diagnostic_failure(self):
        rows = [self.row("10", "Each Box", "S1", "A", "missing", "Small")]
        result = self.build(rows)
        w_row = result["tables"][mapping.OUTPUT_TAB_OTHER_W]["rows"][0]
        self.assertEqual(w_row[0], "")
        self.assertEqual(w_row[2], mapping.SUCCESS_NO)
        self.assertIn("WHOLESALE_NOT_FOUND", w_row[3])
        self.assertIn("source Product ID=10", w_row[3])

    def test_exact_w_duplicates_are_removed_at_declared_grain(self):
        rows = [
            self.row("10", "Each Box", "S1", "A", "same", "Small"),
            self.row("10", "Each Box", "S1", "A2", "same", "Small"),
            self.row("110", "Wholesale", "", "", "same", ""),
        ]
        result = self.build(rows)
        group_w = result["tables"][mapping.OUTPUT_TAB_GROUP_W]["rows"]
        self.assertEqual(len(group_w), 1)
        self.assertEqual(result["summary"]["dedupe_group_w"], 1)

    def test_multiple_desired_values_warn_but_remain_successful(self):
        rows = [
            self.row("10", "Each Box", "S1", "A", "same", "Small"),
            self.row("20", "Each Box", "S1", "B", "b", "Medium"),
            self.row("30", "Each Box", "S2", "C", "same", "Small"),
            self.row("110", "Wholesale", "", "", "same", ""),
        ]
        result = self.build(rows)
        group_w = result["tables"][mapping.OUTPUT_TAB_GROUP_W]["rows"]
        conflicts = [row for row in group_w if row[0] == "110"]
        self.assertEqual(len(conflicts), 2)
        self.assertTrue(all(row[2] == mapping.SUCCESS_YES for row in conflicts))
        self.assertTrue(all("WHOLESALE_MULTIPLE_DESIRED_VALUES" in row[3] for row in conflicts))

    def test_size_order_rejects_conflicting_business_contract(self):
        with self.assertRaisesRegex(ValueError, "multiple sizes"):
            mapping.build_size_order([
                {"排序": "1", "size": "Small"},
                {"排序": "1", "size": "Medium"},
            ])


if __name__ == "__main__":
    unittest.main()
