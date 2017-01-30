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
        obj = queries.QueryBuilder().create(params['op'])
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
        return self._params['name']

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
    def __init__(self, tree):
        self.operations = OrderedDict()
        self.traverse_post_order(tree)

    def traverse_post_order(self, node):
        sub_operations = {}
        for sub_node in node.sub_nodes:
            sub_node, sub_obj_op = self.traverse_post_order(sub_node)
            sub_operations[sub_node.data['name']] = sub_obj_op

        obj_op = Operation(node.data, sub_operations)
        self.operations[node.data['name']] = obj_op
        return node, obj_op

    def get(self):
        return self.operations

    def drop_all_tables(self):
        for op in self.operations.values():
            op.delete()
