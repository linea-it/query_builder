from collections import OrderedDict
from random import randint
import networkx as nx
from threading import Thread, Lock
import copy

from model import intermediate_table


def create_workflow_inverse_edges(graph):
    inv_graph = nx.DiGraph()
    for edge in list(graph.edges()):
        inv_graph.add_edge(edge[1], edge[0])
    return inv_graph


def nodes_no_neighborhood(graph, nodes):
    _nodes = []
    for node in nodes:
        if len(nx.edges(graph, node)) == 0:
            _nodes.append(node)
    return _nodes

"""
    Receiving a tree of operations, this class builds all the operations,
managing the construction in the right order allowing parallelism.
"""


class QueryBuilder():
    lock = Lock()

    def __init__(self, op_description, workflow, thread_pools=1):
        self.operations = OrderedDict()

        self.op_description = op_description
        self.thread_pools = thread_pools

        self.workflow = workflow
        self.inv_workflow = create_workflow_inverse_edges(self.workflow)
        self.updated_workflow = copy.deepcopy(self.workflow)

        self.create_tables(nodes_no_neighborhood(self.updated_workflow,
                                                 self.updated_workflow.nodes()))

    def create_tables(self, nodes):
        if nodes:
            thread_list = []
            for node in nodes:
                t = Thread(target=self._run, args=(node,))
                thread_list.append(t)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()
        return

    def _run(self, _id):

        sub_ops_list = [i[1] for i in nx.edges(self.workflow, _id)]
        sub_ops_dict = {k:self.operations[k] for k in sub_ops_list if k in self.operations}
        self.op_description[_id]['name'] = _id
        obj_op = intermediate_table.IntermediateTable(self.op_description[_id],
                                                      sub_ops_dict)
        self.operations[_id] = obj_op

        for node in nx.edges(self.inv_workflow, _id):
            self.updated_workflow.remove_edge(node[1], node[0])

        with QueryBuilder.lock:
            nodes = nodes_no_neighborhood(self.updated_workflow,
                                          self.inv_workflow.neighbors(_id))

        self.create_tables(nodes)
        return

    def get(self):
        """
        Returns all the operations through a ordereddict where the keys are
        the operations name and the value is the operation.
        """
        return self.operations

    def drop_all_tables(self):
        for op in self.operations.values():
            op.delete()
