import unittest

from core.connection_utils import (
    build_sqlalchemy_url,
    parse_stream_name,
)


class ConnectionUtilsTests(unittest.TestCase):
    def test_parse_stream_name_supports_dash_and_dot(self) -> None:
        dash_ref = parse_stream_name("public-users")
        dot_ref = parse_stream_name("analytics.orders")

        self.assertEqual(dash_ref.namespace, "public")
        self.assertEqual(dash_ref.name, "users")
        self.assertEqual(dot_ref.namespace, "analytics")
        self.assertEqual(dot_ref.name, "orders")

    def test_build_postgres_sqlalchemy_url(self) -> None:
        url = build_sqlalchemy_url(
            "postgres",
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "analytics",
                "username": "etl",
                "password": "secret",
                "ssl_mode": "require",
            },
        )

        self.assertEqual(
            url,
            "postgresql+psycopg2://etl:secret@db.example.com:5432/analytics?sslmode=require",
        )

    def test_build_mysql_sqlalchemy_url(self) -> None:
        url = build_sqlalchemy_url(
            "mysql",
            {
                "host": "mysql.internal",
                "port": 3306,
                "database": "analytics",
                "username": "etl",
                "password": "secret",
            },
        )

        self.assertEqual(
            url,
            "mysql+pymysql://etl:secret@mysql.internal:3306/analytics",
        )


if __name__ == "__main__":
    unittest.main()
