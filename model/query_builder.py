from collections import OrderedDict
from multiprocessing.dummy import Pool as ThreadPool

from model import intermediate_table


"""
    Receiving a tree of operations, this class builds all the operations,
managing the construction in the right order allowing parallelism.
"""


class QueryBuilder():
    def __init__(self, tree, thread_pools=1):
        self.thread_pools = thread_pools
        self.operations = OrderedDict()
        node, obj_op = self.traverse_post_order(tree)

        # drop temporary tables. ROOT node.
        if node.data['permanent_table'] is not True:
            obj_op.delete()
            del self.operations[node.data['name']]

    def traverse_post_order(self, node):
        sub_operations = {}
        if node.sub_nodes:
            pool = ThreadPool(self.thread_pools)
            results = pool.map(self.traverse_post_order, node.sub_nodes)
            pool.close()
            pool.join()
            for result in results:
                sub_operations[result[0].data['name']] = result[1]

        obj_op = intermediate_table.IntermediateTable(node.data,
                                                      sub_operations)

        # drop temporary tables.
        for sub_node in node.sub_nodes:
            if sub_node.data['permanent_table'] is not True:
                sub_operations[sub_node.data['name']].delete()
                del self.operations[sub_node.data['name']]

        self.operations[node.data['name']] = obj_op
        return node, obj_op

    def get(self):
        """
        Returns all the operations through a ordereddict where the keys are
        the operations name and the value is the operation.
        """
        return self.operations

    def drop_all_tables(self):
        for op in self.operations.values():
            op.delete()
