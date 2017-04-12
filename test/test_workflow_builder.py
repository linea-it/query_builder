import unittest
import networkx as nx

import model.workflow_builder as wh


class TestWorkflowBuilder(unittest.TestCase):
    def test_regular_tree(self):
        G = nx.DiGraph()
        G.add_edges_from([(1, 2), (1, 3), (1, 4), (1, 5), (2, 6), (3, 6),
                          (4, 6), (5, 7)])
        workflow = wh.WorkflowBuilder(G)
        self.assertEqual(sorted(workflow.get().edges()), sorted(G.edges()))

    def test_regular_tree_root_defined(self):
        G = nx.DiGraph()
        G.add_edges_from([(1, 2), (1, 3), (1, 4), (1, 5), (2, 6), (3, 6),
                          (4, 6), (5, 7)])
        workflow = wh.WorkflowBuilder(G, root_node=5)

        G2 = nx.DiGraph()
        G2.add_edges_from([(5, 7)])

        self.assertEqual(sorted(workflow.get().edges()), sorted(G2.edges()))

    def test_multiples_root_nodes(self):
        G = nx.DiGraph()
        G.add_edges_from([(1, 2), (1, 3), (1, 4), (5,7)])

        with self.assertRaises(Exception):
            wh.WorkflowBuilder(G)

    def test_cycles(self):
        G = nx.DiGraph()
        G.add_edges_from([(1, 2), (1, 3), (1, 4), (4, 1)])

        with self.assertRaises(Exception):
            wh.WorkflowBuilder(G)

if __name__ == '__main__':
    unittest.main()