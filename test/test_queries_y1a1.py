import unittest
from sqlalchemy.engine import reflection
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy import create_engine

from utils.db import dal, DataAccessLayer

from model.query_builder import QueryBuilder
import model.tree as t
from utils import util


class test_operations(unittest.TestCase):
    def setUp(self):
        # set initial configs.
        db = {
            'dialect': 'postgresql',
            'driver': 'psycopg2',
            'username': 'gavo',
            'password': 'gavo',
            'host': 'localhost',
            'port': '25432',
        }
        self._schema_output = 'tst_oracle_output'

        # Recreate schema
        str_con = DataAccessLayer.str_connection(db)
        self.eng = create_engine(str_con)

        insp = reflection.Inspector.from_engine(self.eng)
        if self._schema_output in insp.get_schema_names():
            self.eng.execute(DropSchema(self._schema_output, cascade=True))
        self.eng.execute(CreateSchema(self._schema_output))

        dal.db_init(str_con, schema_output=self._schema_output)
        ops_path = "test/ops_y1a1.json"
        self.ops_desc = util.load_json(ops_path)
        self.tree_desc = {
            "galaxy_properties": ["photoz"],
            "photoz": ["sg_separation"],
            "sg_separation": ["object_selection"],
            "object_selection": ["bitmask"],
            "bitmask": ["cuts"],
            "cuts": ["reduction", "zero_point"],
            "reduction": ["footprint"],
            "footprint": ["exposure_time", "mangle_map", "bad_regions"],
            "exposure_time": ["exposure_time_i", "exposure_time_r",
                              "exposure_time_z"],
            "mangle_map": ["mangle_maps_i", "mangle_maps_r"]
        }
        self.operations = None

    def test_op_great_equal(self):
        tree = t.Tree(self.tree_desc,
                      self.ops_desc).tree_builder('exposure_time_i')
        self.operations = QueryBuilder(tree)

    def test_op_footprint_inner_left(self):
        tree = t.Tree(self.tree_desc,
                      self.ops_desc).tree_builder('footprint')
        self.operations = QueryBuilder(tree)

    def test_op_combined_maps(self):
        tree = t.Tree(self.tree_desc,
                      self.ops_desc).tree_builder('exposure_time')
        self.operations = QueryBuilder(tree)

    def test_op_zero_point(self):
        tree_desc = {'zero_point': []}
        tree = t.Tree(tree_desc,
                      self.ops_desc).tree_builder('zero_point')
        self.operations = QueryBuilder(tree)

    def test_op_object_selection(self):
        tree = t.Tree(self.tree_desc,
                      self.ops_desc).tree_builder('object_selection')
        self.operations = QueryBuilder(tree)

    def test_op_sg_separation(self):
        tree = t.Tree(self.tree_desc,
                      self.ops_desc).tree_builder('sg_separation')
        self.operations = QueryBuilder(tree)

    def test_op_photoz(self):
        tree = t.Tree(self.tree_desc,
                      self.ops_desc).tree_builder('photoz')
        self.operations = QueryBuilder(tree)

    def test_op_galaxy_properties(self):
        tree = t.Tree(self.tree_desc,
                      self.ops_desc).tree_builder('galaxy_properties')
        self.operations = QueryBuilder(tree)

    def tearDown(self):
        insp = reflection.Inspector.from_engine(self.eng)
        if self._schema_output in insp.get_schema_names():
            self.eng.execute(DropSchema(self._schema_output, cascade=True))


if __name__ == '__main__':
    unittest.main()
