import unittest
from mock import patch, Mock

from utils.db import dal, DataAccessLayer

from model import operations as ops


class test_operations(unittest.TestCase):
    db = {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'gavo',
        'password': 'gavo',
        'host': 'localhost',
        'port': '25432',
    }
    dal.db_init(DataAccessLayer.str_connection(db))
    base_path = "test/config_devel2/"

    @staticmethod
    def get_operations(file_name):
        path = test_operations.base_path + file_name
        return ops.OperationsBuilder(
            ops.OperationsBuilder.json_to_ordered_dict(path))

    def setUp(self):
        self.operations = None

    def test_op_great_equal(self):
        self.operations = test_operations.get_operations('great_equal.json')

    def test_op_footprint_inner_left(self):
        self.operations = test_operations.get_operations(
                'footprint_inner_left.json')

    def test_op_object_selection(self):
        self.operations = test_operations.get_operations(
                'object_selection.json')

    def test_op_sg_separation(self):
        self.operations = test_operations.get_operations(
                'sg_separation.json')

    def test_op_photoz(self):
        self.operations = test_operations.get_operations(
                'photoz.json')

    def tearDown(self):
        self.operations.drop_all_tables()


if __name__ == '__main__':
    unittest.main()
