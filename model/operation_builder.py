from model import operations


"""
    This is the Factory class to create new operations.
    
    When a new operation is defined in models.opetations, we must add here the
condition to create the apropriate class.
"""


class OperationBuilder():
    @staticmethod
    def create(operation_type):
        if operation_type == operations.GreatEqual.OPERATION:
            operation = operations.GreatEqual()
        elif operation_type == operations.CombinedMaps.OPERATION:
            operation = operations.CombinedMaps()
        elif operation_type == operations.BadRegions.OPERATION:
            operation = operations.BadRegions()
        elif operation_type == operations.Footprint.OPERATION:
            operation = operations.Footprint()
        elif operation_type == operations.ObjectSelection.OPERATION:
            operation = operations.ObjectSelection()
        elif operation_type == operations.SgSeparation.OPERATION:
            operation = operations.SgSeparation()
        elif operation_type == operations.PhotoZ.OPERATION:
            operation = operations.PhotoZ()
        elif operation_type == operations.GalaxyProperties.OPERATION:
            operation = operations.GalaxyProperties()
        else:
            raise "This operation is not implemented."

        return operation
