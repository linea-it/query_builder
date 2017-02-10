import unittest
from mock import patch, Mock

from utils.db import dal, DataAccessLayer

from model.query_builder import QueryBuilder
import model.tree as t
from utils import util


class test_operations(unittest.TestCase):
    db = {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'postgres',
        'password': 'tet123456',
        'host': 'localhost',
        'port': '5432',
        'database': 'query_builder'
    }
    dal.db_init(DataAccessLayer.str_connection(db),
                schema_input='tst_oracle_input',
                schema_output='tst_oracle_output')
    base_path = "test/config_y1a1_subset/"

    @staticmethod
    def get_operations(file_name):
        path = test_operations.base_path + file_name
        obj = util.load_json(path)
        tree = t.tree_builder(obj)
        return QueryBuilder(tree)

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

    def test_op_galaxy_properties(self):
        self.operations = test_operations.get_operations(
                'galaxy_properties.json')

    def tearDown(self):
        self.operations.drop_all_tables()


if __name__ == '__main__':
    unittest.main()
