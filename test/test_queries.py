import unittest
from mock import patch, Mock

from utils.db import dal, str_connection

from model import operations as ops


class test_operations(unittest.TestCase):
    dal.db_init(str_connection())
    base_path = "test/config/"

    @staticmethod
    def get_operations(file_name):
        path = test_operations.base_path + file_name
        return ops.OperationsBuilder(
            ops.OperationsBuilder.json_to_ordered_dict(path))

    def setUp(self):
        self.operations = None

    def test_op_great_equal(self):
        self.operations = test_operations.get_operations('great_equal.json')

    def test_op_bad_regions(self):
        self.operations = test_operations.get_operations('bad_regions.json')

    def test_op_combined_maps(self):
        self.operations = test_operations.get_operations('combined_maps.json')

    def test_op_footprint(self):
        self.operations = test_operations.get_operations('footprint.json')

    def test_op_footprint_inner(self):
        self.operations = test_operations.get_operations(
                'footprint_inner.json')

    def test_op_footrpint_left(self):
        self.operations = test_operations.get_operations(
                'footprint_left.json')

    def test_op_footprint_inner_left(self):
        self.operations = test_operations.get_operations(
                'footprint_inner_left.json')

    def tearDown(self):
        self.operations.drop_all_tables()


if __name__ == '__main__':
    unittest.main()
