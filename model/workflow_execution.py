from threading import Thread, Lock
import copy


"""
    Given a graph -workflow-, this class provides a callback to the nodes that
are ready to execute and manages the parallelism.
"""


def nodes_no_predecessors(graph, nodes):
    return [node for node in nodes if len(graph.predecessors(node)) == 0]


def nodes_no_successors(graph, nodes):
    return [node for node in nodes if len(graph.successors(node)) == 0]


class WorkflowExecution():

    def __init__(self, workflow_builder, set_event_node_ready,
                 set_event_node_free):
        """
        Args:
            set_event_node_ready - A callback called from workflow_execution
            when a node is ready to execute.
            set_event_node_free - A callback called from workflow_execution
            when a node is no used by others nodes.
        """
        self.lock = Lock()
        self._set_event_node_ready = set_event_node_ready
        self._set_event_node_free = set_event_node_free

        self.workflow = workflow_builder.get()

        self.updated_workflow = copy.deepcopy(self.workflow)
        self.create_tables(nodes_no_successors(self.updated_workflow,
                                               self.updated_workflow.nodes()))

        # root node
        self._set_event_node_free([workflow_builder.get_root_node()])

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

    def _run(self, node):
        self._set_event_node_ready(node)

        # remove predecessors edges
        for _node in self.workflow.predecessors(node):
            self.updated_workflow.remove_edge(_node, node)

        with self.lock:
            nodes = nodes_no_successors(self.updated_workflow,
                                        self.workflow.predecessors(node))
            self._set_event_node_free(
                nodes_no_predecessors(self.updated_workflow,
                                      self.workflow.successors(node)))

        self.create_tables(nodes)
