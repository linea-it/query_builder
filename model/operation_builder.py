from model import operations as op


"""
    This is the Factory class to create new operations.

    When a new operation is defined in models.opetations, we must add here the
condition to create the apropriate class.
"""


class OperationBuilder():
    ops = {}
    ops[op.GreatEqual.OPERATION] = op.GreatEqual()
    ops[op.CombinedMaps.OPERATION] = op.CombinedMaps()
    ops[op.BadRegions.OPERATION] = op.BadRegions()
    ops[op.Footprint.OPERATION] = op.Footprint()
    ops[op.Reduction.OPERATION] = op.Reduction()
    ops[op.Cuts.OPERATION] = op.Cuts()
    ops[op.Bitmask.OPERATION] = op.Bitmask()
    ops[op.ZeroPoint.OPERATION] = op.ZeroPoint()
    ops[op.ObjectSelection.OPERATION] = op.ObjectSelection()
    ops[op.SgSeparation.OPERATION] = op.SgSeparation()
    ops[op.PhotoZ.OPERATION] = op.PhotoZ()
    ops[op.GalaxyProperties.OPERATION] = op.GalaxyProperties()

    @staticmethod
    def create(operation_type):
        try:
            operation = OperationBuilder.ops[operation_type]
            return operation
        except:
            raise "The %s operation is not implemented." % operation_type
