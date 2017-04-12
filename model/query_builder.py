from collections import OrderedDict

from model import workflow_builder as wb
from model import workflow_execution as we
from model import intermediate_table


"""
    Receiving a tree of operations, this class builds all the operations,
managing the construction in the right order allowing parallelism.
"""


class QueryBuilder():
    def __init__(self, op_description, workflow, root_node=None):
        self.operations = OrderedDict()
        self.op_description = op_description

        self.workflow = wb.WorkflowBuilder(workflow,
                                           root_node=root_node).get()

        we.WorkflowExecution(self.workflow, self.set_event_node_ready)

    # callback - it is called from workflow_execution when a node is ready to execute.
    def set_event_node_ready(self, _id):
        sub_ops_list = self.workflow.successors(_id)
        sub_ops_dict = {k:self.operations[k] for k in sub_ops_list if k in self.operations}
        self.op_description[_id]['name'] = _id
        obj_op = intermediate_table.IntermediateTable(self.op_description[_id],
                                                      sub_ops_dict)
        self.operations[_id] = obj_op

    def get_operations(self):
        """
        Returns all the operations through a ordereddict where the keys are
        the operations name and the value is the operation.
        """
        return self.operations

    def drop_all_tables(self):
        for op in self.operations.values():
            op.delete()
