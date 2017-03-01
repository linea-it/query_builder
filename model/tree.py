"""
It has the data_structure Node and methods to facilitate the creation of trees.
"""


class Node(object):
    """
    It represents the basic data structure of a tree.
    """
    def __init__(self, data=None, level=0):
        self.data = data
        self.level = level
        self.sub_nodes = []

    def __repr__(self):
        return '\n{indent}Node({name},{sub_nodes})'.format(
                                         indent=self.level*'\t',
                                         name=self.data['name'],
                                         sub_nodes=repr(self.sub_nodes))

    def add_child(self, node):
        self.sub_nodes.append(node)


def traverse_post_order(node):
    """
    Given a node, it returns the nodes in post order sequence.
    """
    for sub_node in node.sub_nodes:
        traverse_post_order(sub_node)
    return node


class Tree():
    def __init__(self, tree_desc, ops_desc):
        self.tree_desc = tree_desc
        self.ops_desc = ops_desc

    def tree_builder(self, root_node, level=0):
        """
        Creates a tree using a dict -obj- as input.
        """

        # The operation name can be accessed using the key 'name'.
        cur_ops_desc = self.ops_desc[root_node]['name'] = root_node
        node = Node(data=self.ops_desc[root_node], level=level)
        if root_node in self.tree_desc:
            for sub_node in self.tree_desc[root_node]:
                node.add_child(self.tree_builder(sub_node, level=level+1))
        return node
