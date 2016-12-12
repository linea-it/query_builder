import unittest
from mock import patch, Mock

from utils.db import dal, str_connection
import test.db.create_tables as ct

from model import queries


class test_db(unittest.TestCase):
    dal.db_init(str_connection())
    ct.create_all_tables()
    dal.load_tables()
    print(dal.tables)

    def test_systematic_maps(self):
        exp_time = queries.Operation('exposure_time')
        exp_time.create()

    def test_bad_regions(self):
        bad_regions = queries.Operation('bad_regions')
        bad_regions.create()

if __name__ == '__main__':
    unittest.main()
