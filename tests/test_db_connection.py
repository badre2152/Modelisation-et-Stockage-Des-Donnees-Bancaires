"""Database URL construction regression tests."""

import os
import unittest
from unittest.mock import patch

from sqlalchemy.engine import URL
from database_pipeline_py.db_connection import get_engine


class DatabaseUrlTests(unittest.TestCase):
    @patch("database_pipeline_py.db_connection.create_engine")
    def test_special_characters_are_preserved_as_credentials(self, factory):
        settings = {
            "DB_USER": "analyst@team",
            "DB_PASSWORD": "p@ss:word/with?symbols",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_NAME": "financecore",
        }
        with patch.dict(os.environ, settings):
            get_engine()
        url = factory.call_args.args[0]
        self.assertIsInstance(url, URL)
        self.assertEqual(url.username, settings["DB_USER"])
        self.assertEqual(url.password, settings["DB_PASSWORD"])
        self.assertEqual(url.database, settings["DB_NAME"])
        self.assertEqual(url.port, 5432)

    @patch("database_pipeline_py.db_connection.create_engine")
    def test_missing_password_fails_before_opening_connection(self, factory):
        settings = {
            "DB_USER": "analyst",
            "DB_PASSWORD": "",
            "DB_NAME": "financecore",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
        }
        with patch.dict(os.environ, settings):
            with self.assertRaisesRegex(ValueError, "Missing required DB env vars"):
                get_engine()
        factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
