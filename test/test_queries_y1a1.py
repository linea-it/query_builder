import unittest
import networkx as nx
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

        self.recreate_schema()

        dal.db_init(str_con, schema_output=self._schema_output)
        ops_path = "test/ops_y1a1.json"
        self.ops_desc = util.load_json(ops_path)
        self.workflow = nx.read_adjlist('test/operations_sequence.al',
                                        create_using=nx.DiGraph())

        self.operations = None

    def recreate_schema(self):
        insp = reflection.Inspector.from_engine(self.eng)
        if self._schema_output in insp.get_schema_names():
            self.eng.execute(DropSchema(self._schema_output, cascade=True))
        self.eng.execute(CreateSchema(self._schema_output))

    def query_builder_factory(self, operation):
        return QueryBuilder(self.ops_desc, self.workflow, root_node=operation)

    def test_op_great_equal(self):
        self.operations = self.query_builder_factory('mangle_maps_i')

    def test_op_footprint_inner_left(self):
        self.operations = self.query_builder_factory('footprint')

    def test_op_combined_maps(self):
        self.operations = self.query_builder_factory('mangle_map')

    def test_op_zero_point(self):
        self.operations = self.query_builder_factory('zero_point')

    def test_op_object_selection(self):
        self.operations = self.query_builder_factory('object_selection')

    def test_op_sg_separation(self):
        self.operations = self.query_builder_factory('sg_separation')

    def test_op_photoz(self):
        self.operations = self.query_builder_factory('photoz')

    def test_op_galaxy_properties(self):
        self.operations = self.query_builder_factory('galaxy_properties')

    def tearDown(self):
        insp = reflection.Inspector.from_engine(self.eng)
        if self._schema_output in insp.get_schema_names():
            self.eng.execute(DropSchema(self._schema_output, cascade=True))


if __name__ == '__main__':
    unittest.main()
