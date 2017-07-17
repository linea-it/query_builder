from model import operations as op


"""
    This is the Factory class to create new operations.

    When a new operation is defined in models.opetations, we must add here the
condition to create the apropriate class.
"""


class OperationBuilder():
    ops = {}
    ops['great_equal'] = op.GreatEqual()
    ops['join'] = op.CombinedMaps()
    ops['bad_regions'] = op.BadRegions()
    ops['footprint'] = op.Footprint()
    ops['reduction'] = op.Reduction()
    ops['cuts'] = op.Cuts()
    ops['bitmask'] = op.Bitmask()
    ops['zero_point'] = op.ZeroPoint()
    ops['object_selection'] = op.ObjectSelection()
    ops['sg_separation'] = op.SgSeparation()
    ops['photoz'] = op.PhotoZ()
    ops['galaxy_properties'] = op.GalaxyProperties()

    @staticmethod
    def create(operation_type):
        try:
            operation = OperationBuilder.ops[operation_type]
            return operation
        except:
            raise "The %s operation is not implemented." % operation_type
