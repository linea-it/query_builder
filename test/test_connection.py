import unittest
from mock import patch, Mock

from utils import db_connection


class test_db_connection(unittest.TestCase):
    def setUp(self):
        self.con = db_connection.DbConnection()

    def test_db_successfully_conection(self):
        self.assertTrue(self.con.is_connection_available())

    # def test_db_disconected(self):
    #     self.assertTrue(self.con.is_connection_available())

    @patch.object(db_connection.DbConnection, "_load_settings",
                  return_value={
                    'dialects': 'postgresql',
                    'driv': 'psycopg2',
                    'password': 'tet123456'
                  })
    def test_invalid_input_format(self, mock_input):
        self.con = db_connection.DbConnection()
        self.assertRaises(KeyError, lambda: self.con.str_connection())

    @patch.object(db_connection.DbConnection, "_load_settings",
                  return_value={
                    'dialect': 'postgresql',
                    'driver': 'psycopg2',
                    'username': 'lucasdpn',
                    'password': 'tet123456',
                    'host': 'localhost',
                    'port': '5432',
                    'database': 'db_sql_alchemy'
                  })
    def test_valid_input_format(self, mock_input):
        self.con = db_connection.DbConnection()
        self.assertEqual(
            self.con.str_connection(),
            ("postgresql+psycopg2://"
             "lucasdpn:tet123456@localhost:5432/db_sql_alchemy"))

if __name__ == '__main__':
    unittest.main()
