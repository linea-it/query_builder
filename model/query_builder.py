from collections import OrderedDict

from model import workflow_builder as wb
from model import workflow_execution as we
from model import intermediate_table


"""
    Receiving a tree of operations, this class builds all the operations,
managing the construction in the right order allowing parallelism.
"""


class QueryBuilder:
    def __init__(self, op_description, workflow, root_node=None):
        self.operations = OrderedDict()

        self.op_description = op_description
        self.workflow_builder = wb.WorkflowBuilder(workflow,
                                                   root_node=root_node)
        self.workflow = self.workflow_builder.get()

        we.WorkflowExecution(self.workflow_builder, self.set_event_node_ready,
                             self.set_event_node_free)

    def set_event_node_ready(self, node):
        sub_ops_list = self.workflow.successors(node)
        sub_ops_dict = {k: self.operations[k] for k in sub_ops_list
                        if k in self.operations}
        self.op_description[node]['name'] = node
        obj_op = intermediate_table.IntermediateTable(
                self.op_description[node], sub_ops_dict)
        self.operations[node] = obj_op

    def set_event_node_free(self, nodes):
        for node in nodes:
            if not self.op_description[node]['permanent_table']:
                self.operations[node].delete()
                del(self.operations[node])

    def get_operations(self):
        """
        Returns all the operations using a ordereddict. The keys are
        the operations name and the value is the operation.
        """
        return self.operations

    def drop_all_tables(self):
        for op in self.operations.values():
            op.delete()
