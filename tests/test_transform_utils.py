import unittest

from core.transform_utils import (
    apply_record_transform,
    compile_transform_fn,
    count_data_item_rows,
    extract_transform_output_mappings,
)


class TransformUtilsTests(unittest.TestCase):
    def test_apply_record_transform_renames_and_drops(self) -> None:
        result = apply_record_transform(
            {"id": 1, "old_name": "Ada", "drop_me": True},
            column_map={"old_name": "name"},
            drop_columns={"drop_me"},
        )

        self.assertEqual(result, {"id": 1, "name": "Ada"})

    def test_apply_record_transform_skip_policy_on_error(self) -> None:
        fn = compile_transform_fn(
            """
def transform(record):
    raise RuntimeError("boom")
"""
        )

        result = apply_record_transform(
            {"id": 1},
            transform_fn=fn,
            on_error="skip",
        )

        self.assertIsNone(result)

    def test_extract_transform_output_mappings(self) -> None:
        mappings = extract_transform_output_mappings(
            """
def transform(record):
    row_id = record.get("id")
    return {"user_id": row_id, "email": record["email"]}
"""
        )

        self.assertEqual(mappings, {"user_id": "id", "email": "email"})

    def test_count_data_item_rows_for_collections(self) -> None:
        self.assertEqual(count_data_item_rows([{"id": 1}, {"id": 2}]), 2)
        self.assertEqual(count_data_item_rows({"id": 1}), 1)


if __name__ == "__main__":
    unittest.main()
