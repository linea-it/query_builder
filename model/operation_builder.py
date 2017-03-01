from model import operations


"""
    This is the Factory class to create new operations.

    When a new operation is defined in models.opetations, we must add here the
condition to create the apropriate class.
"""


class OperationBuilder():
    ops = {}
    ops[operations.GreatEqual.OPERATION] = operations.GreatEqual()
    ops[operations.CombinedMaps.OPERATION] = operations.CombinedMaps()
    ops[operations.BadRegions.OPERATION] = operations.BadRegions()
    ops[operations.Footprint.OPERATION] = operations.Footprint()
    ops[operations.Reduction.OPERATION] = operations.Reduction()
    ops[operations.Cuts.OPERATION] = operations.Cuts()
    ops[operations.Bitmask.OPERATION] = operations.Bitmask()
    ops[operations.ObjectSelection.OPERATION] = operations.ObjectSelection()
    ops[operations.SgSeparation.OPERATION] = operations.SgSeparation()
    ops[operations.PhotoZ.OPERATION] = operations.PhotoZ()
    ops[operations.GalaxyProperties.OPERATION] = operations.GalaxyProperties()

    @staticmethod
    def create(operation_type):
        try:
            operation = OperationBuilder.ops[operation_type]
            return operation
        except:
            raise "The %s operation is not implemented." % operation_type
