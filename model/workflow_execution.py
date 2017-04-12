import networkx as nx
from threading import Thread, Lock
import copy


"""
    Given a graph -workflow-, this class provides a callback to the nodes that
are ready to execute and manages the parallelism.
"""


def nodes_no_neighborhood(graph, nodes):
    _nodes = []
    for node in nodes:
        if len(nx.edges(graph, node)) == 0:
            _nodes.append(node)
    return _nodes


class WorkflowExecution():

    def __init__(self, workflow, set_event_node_ready):
        self.lock = Lock()

        self._set_event_node_ready = set_event_node_ready
        self.workflow = workflow

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
        self._set_event_node_ready(_id)

        for node in self.workflow.predecessors(_id):
            self.updated_workflow.remove_edge(node, _id)

        with self.lock:
            nodes = nodes_no_neighborhood(self.updated_workflow,
                                          self.workflow.predecessors(_id))

        self.create_tables(nodes)
        return
