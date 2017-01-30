class Node(object):
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
    for sub_nodes in node.sub_nodes:
        traverse_post_order(sub_nodes)
    print(node.data['name'])
    return node


def tree_builder(obj, level=0):
    node = Node(data=obj, level=level)
    for child in obj.get('sub_op', []):
        node.add_child(tree_builder(child, level=level+1))
    return node
