import unittest

from core.config_builder import build_tap_config, build_target_config, resolve_tap_type, resolve_target_type


class BuildTargetConfigTests(unittest.TestCase):
    def test_source_aliases_resolve_without_special_case_route_logic(self) -> None:
        self.assertEqual(resolve_tap_type("source-postgres"), "postgres")
        self.assertEqual(resolve_tap_type("postgresql"), "postgres")
        self.assertEqual(resolve_target_type("target-postgres"), "postgres")

        config = build_tap_config(
            {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "secret",
                "database": "analytics",
            },
            replication_slot_name="slot_123",
            source_type="postgresql",
        )

        self.assertEqual(config["replication_slot_name"], "slot_123")
        self.assertIn("sqlalchemy_url", config)

    def test_replace_mode_maps_to_overwrite_and_aliases_destination_table(self) -> None:
        config = build_target_config(
            {
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "secret",
                "database": "analytics",
            },
            "warehouse",
            source_stream="source_locations",
            dest_table="dim_locations",
            emit_method="replace",
            dest_type="target-postgres",
        )

        self.assertEqual(config["load_method"], "overwrite")
        self.assertEqual(
            config["stream_maps"],
            {"source_locations": {"__alias__": "dim_locations"}},
        )
        self.assertNotIn("emit_method", config)
        self.assertNotIn("upsert_key", config)

    def test_unregistered_connector_type_fails_fast(self) -> None:
        with self.assertRaises(ValueError):
            resolve_tap_type("source-mysql")


if __name__ == "__main__":
    unittest.main()
