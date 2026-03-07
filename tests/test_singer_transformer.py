import unittest

from core.singer_transformer import (
    _map_key_properties,
    _transform_schema,
    transform_record,
)


class SingerTransformerTests(unittest.TestCase):
    def test_transform_record_drops_records_when_transform_returns_none(self) -> None:
        transformed = transform_record(
            {"location_id": "abc"},
            {},
            set(),
            lambda record: None,
        )

        self.assertIsNone(transformed)

    def test_transform_schema_uses_transformed_keys_and_source_types(self) -> None:
        schema = {
            "properties": {
                "location_id": {"type": ["string", "null"]},
                "id": {"type": "string"},
            },
            "required": ["id"],
        }

        transformed_schema = _transform_schema(
            schema,
            {"nexusLocationId": "abc", "isActive": True},
            {"nexusLocationId": "location_id"},
            {},
        )

        self.assertEqual(
            transformed_schema["properties"]["nexusLocationId"]["type"],
            ["string", "null"],
        )
        self.assertEqual(
            transformed_schema["properties"]["isActive"]["type"],
            ["boolean", "null"],
        )

    def test_key_properties_follow_output_column_names(self) -> None:
        key_properties = _map_key_properties(
            ["location_id"],
            {"nexusLocationId": "location_id"},
            ["nexusLocationId"],
        )

        self.assertEqual(key_properties, ["nexusLocationId"])


if __name__ == "__main__":
    unittest.main()
