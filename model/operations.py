import os
import json
from collections import OrderedDict

from sqlalchemy.sql import select
from sqlalchemy import Table

import queries
from model import sql_operations as op

from utils.db import dal

class Operation():
    def __init__(self, params, sub_operations):
        self._params = params
        self._sub_operations = sub_operations

        # define operation
        self._stm = None
        key, value = list(params.items())[0]
        self._set_operation(value['op'])

        # create temp table to let the data accessible.
        self.create()
        self.data_table = self._access_data_table()

    def _set_operation(self, operation_type):
        if operation_type == queries.GreatEqual.OP:
            op = queries.GreatEqual()
        elif operation_type == queries.CombinedMaps.OP:
            op = queries.CombinedMaps()
        else:
            raise "This operations is not registered"

        self._stm = op.get_statement(self._params, self._sub_operations)

    def __str__(self):
        return (str(self._stm))

    def operation_name(self):
        return list(self._params.keys())[0]

    def save_at(self):
        return self.operation_name() + "_" + "table"

    def create(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.CreateTableAs(self.save_at(),
                        self._stm))

    def _access_data_table(self):
        with dal.engine.connect() as con:
            table = Table(self.save_at(), dal.metadata, autoload=True)
            stmt = select([table])
            result = con.execute(stmt).fetchall()
        return result

    def access_data_table(self):
        return self.data_table

    def delete(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.DropTable(self.save_at()))


class OperationBuilder():
    def __init__(self):
        self.operations = OrderedDict()

        data = {}
        with open('inout/data.json') as data_file:
            data = json.load(data_file, object_pairs_hook=OrderedDict)

        self.dfs_pre_order(data['operations'])

    def dfs_pre_order(self, node):
        if 'sub_op' in node:
            sub_node, sub_node_obj = self.dfs_pre_order(node['sub_op'])
            new_tree = {'sub_op': sub_node}
            new_tree_obj = {'sub_op': sub_node_obj}
            return new_tree, new_tree_obj

        if 'op' in node:
            return node, None

        else:
            new_tree = {}
            new_tree_obj = {}
            for key in node.keys():
                sub_node, sub_node_obj = self.dfs_pre_order(node[key])

                new_tree[key] = sub_node
                obj_op = Operation({key: node[key]}, sub_node_obj)
                new_tree_obj[key] = obj_op

                # review
                self.operations[key] = obj_op
            return new_tree, new_tree_obj

    def operations_list(self):
        return self.operations
