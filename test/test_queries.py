import unittest
from mock import patch, Mock
from collections import OrderedDict

from utils.db import dal, str_connection

from model import queries


class test_operations(unittest.TestCase):
    dal.db_init(str_connection())

    def test_op_great_equal(self):
        params = {u'exposure_time_i': OrderedDict([
                   (u'db', u'systematic_map'),
                   (u'op', u'great_equal'),
                   (u'description', u'exposure_time_i description'),
                   (u'band', u'g'),
                   (u'value', u'0.55'),
                   (u'type', u'float')])}
        op = queries.Operation(params, None)
        print(str(op))

if __name__ == '__main__':
    unittest.main()
