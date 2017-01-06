import os
import json
from collections import OrderedDict

from utils.db import dal, str_connection

from model import queries


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
                obj_op = queries.Operation({key: node[key]}, sub_node_obj)
                new_tree_obj[key] = obj_op

                # review
                self.operations[key] = obj_op
            return new_tree, new_tree_obj

    def operations_list(self):
        return self.operations


if __name__ == "__main__":
    dal.db_init(str_connection())
    dal.load_tables()

    builder = OperationBuilder()
    ops = builder.operations_list()

    for k, v in ops.items():
        print(k)
        print(v.access_data_table())
