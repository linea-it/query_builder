import os
import json
from collections import OrderedDict

from sqlalchemy.sql import select
from sqlalchemy import Table

from model import queries
from model import sql_operations as op

from utils.db import dal


class Operation():
    def __init__(self, params, sub_operations):
        self._params = params
        self._sub_operations = sub_operations

        # get query
        obj = queries.QueryBuilder().create(list(params.values())[0]['op'])
        self._query = obj.get_statement(params, sub_operations)

        # create temp table to let the data accessible.
        self.create()

        with dal.engine.connect() as con:
            table = Table(self.save_at(), dal.metadata, autoload=True)
            self._columns = table.c
            stmt = select([table])
            self._data_table = con.execute(stmt).fetchall()

    def __str__(self):
        return (str(self._query))

    def operation_name(self):
        return list(self._params.keys())[0]

    def save_at(self):
        return self.operation_name() + "_" + "table"

    def create(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.CreateTableAs(self.save_at(),
                        self._query))

    def access_data_table(self):
        return self._data_table

    def columns_name(self):
        return self._columns

    def delete(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.DropTable(self.save_at()))


class OperationsBuilder():
    def __init__(self, data):
        self.operations = OrderedDict()
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

    def get(self):
        return self.operations

    def drop_all_tables(self):
        for op in self.operations.values():
            op.delete()

    @staticmethod
    def json_to_ordered_dict(file_path):
        data = {}
        with open(file_path) as data_file:
            data = json.load(data_file, object_pairs_hook=OrderedDict)
        return data
