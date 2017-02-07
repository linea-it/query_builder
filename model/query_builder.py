from model import queries


"""
    This is the Factory class to create new operations.
"""


class QueryBuilder():
    @staticmethod
    def create(operation_type):
        if operation_type == queries.GreatEqual.QUERY:
            query = queries.GreatEqual()
        elif operation_type == queries.CombinedMaps.QUERY:
            query = queries.CombinedMaps()
        elif operation_type == queries.BadRegions.QUERY:
            query = queries.BadRegions()
        elif operation_type == queries.Footprint.QUERY:
            query = queries.Footprint()
        elif operation_type == queries.ObjectSelection.QUERY:
            query = queries.ObjectSelection()
        elif operation_type == queries.SgSeparation.QUERY:
            query = queries.SgSeparation()
        elif operation_type == queries.PhotoZ.QUERY:
            query = queries.PhotoZ()
        elif operation_type == queries.GalaxyProperties.QUERY:
            query = queries.GalaxyProperties()
        else:
            raise "This query is not implemented."

        return query
