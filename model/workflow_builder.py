import networkx as nx


class WorkflowBuilder():
    def __init__(self, workflow, root_node=None):
        self.root_node = root_node
        self.workflow = workflow

        self.define_graph()

        if self.has_cycles():
            raise("Workflow has cycles")

    def has_cycles(self):
        return True if len(list(nx.simple_cycles(self.workflow))) > 0\
                    else False

    def define_graph(self):
        if not self.is_root_node_set():
            self.root_node = self.get_root_node()
        else:
            self.workflow = self.get_subgraph()

    def get_root_node(self):
        root = [n for n, d in self.workflow.in_degree().items() if d == 0]
        if len(root) == 0:
            raise("The tree has no root node.")
        if len(root) > 1:
            raise("The tree has multiple rood nodes.")
        return root[0]

    def get_subgraph(self):
        nodes_to_include = nx.descendants(self.workflow, self.root_node)
        nodes_to_include.add(self.root_node)
        return self.workflow.subgraph(nodes_to_include)

    def is_root_node_set(self):
        return False if self.root_node is None else True

    def get(self):
        return self.workflow
