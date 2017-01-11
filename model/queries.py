from sqlalchemy.sql import select
from sqlalchemy import Table
from sqlalchemy.sql.expression import literal_column

from utils.db import dal

from model import sql_operations


class IQuery():
    def get_statement(self, params, sub_operations):
        raise NotImplementedError("Implement this method")


class QueryBuilder():
    @staticmethod
    def create(operation_type):
        if operation_type == GreatEqual.QUERY:
            query = GreatEqual()
        elif operation_type == CombinedMaps.QUERY:
            query = CombinedMaps()
        elif operation_type == BadRegions.QUERY:
            query = BadRegions()
        elif operation_type == Footprint.QUERY:
            query = Footprint()
        else:
            raise "This query is not implemented."

        return query


class GreatEqual(IQuery):
    QUERY = "great_equal"

    def get_statement(self, params, sub_operations):
        key, value = list(params.items())[0]
        table = Table(value['db'], dal.metadata, autoload=True)
        stm = select(
          [table]).where(table.c.signal >= literal_column(value['value']))
        return stm


class CombinedMaps(IQuery):
    QUERY = 'join'

    def get_statement(self, params, sub_operations):
        sub_op_names = list(params[list(params.keys())[0]]['sub_op'].keys())

        # load tables.
        sub_tables = []
        for table in sub_op_names:
            sub_tables.append(Table(sub_operations['sub_op'][table].save_at(),
                                    dal.metadata, autoload=True))

        # join statement
        stm = select([sub_tables[0]])
        stm_join = sub_tables[0]
        for i in range(1, len(sub_tables)):
            stm_join = stm_join.join(sub_tables[i], sub_tables[i-1].c.pixel ==
                                     sub_tables[i].c.pixel)
        stm = stm.select_from(stm_join)

        # REVIEW
        # drop sub tables
        # for k, v in sub_operations['sub_op'].items():
        #     v.delete()
        return stm


class BadRegions(IQuery):
    QUERY = "bad_regions"

    def get_statement(self, params, sub_operations):
        key, value = list(params.items())[0]
        table = Table(value['db'], dal.metadata, autoload=True)
        stm = select([table]).where(sql_operations.BitwiseAnd(table.c.signal,
                                    literal_column(value['value'])) >
                                    literal_column('0'))
        return stm


class Footprint(IQuery):
    QUERY = 'footprint'

    def get_statement(self, params, sub_operations):
        inner_join = ["exposure_time", "depth_map"]
        left_join = ["bad_regions"]

        inner_join_ops = []
        left_join_ops = []

        # divide operations accordingly
        if sub_operations:
            for k, v in list(sub_operations['sub_op'].items()):
                if k in inner_join:
                    inner_join_ops.append(v)
                elif k in left_join:
                    left_join_ops.append(v)
                else:
                    raise("operations does not exist.")

        # load tables.
        table_footprint = Table(list(params.values())[0]['db'], dal.metadata,
                                autoload=True)
        sub_tables_inner = []
        for table in inner_join_ops:
            sub_tables_inner.append(Table(table.save_at(), dal.metadata,
                                    autoload=True))
        sub_tables_left = []
        for table in left_join_ops:
            sub_tables_left.append(Table(table.save_at(), dal.metadata,
                                   autoload=True))

        stm = select([table_footprint])

        # join statement
        stm_join = table_footprint
        # Inner join
        for table in sub_tables_inner:
            stm_join = stm_join.join(table, table_footprint.c.pixel ==
                                     table.c.pixel)
        # Left Join
        for table in sub_tables_left:
            stm_join = stm_join.join(table, table_footprint.c.pixel ==
                                     table.c.pixel, isouter=True)

        if len(sub_tables_inner) > 0 or len(sub_tables_left) > 0:
            stm = stm.select_from(stm_join)

        if len(sub_tables_left) > 0:
            for table in sub_tables_left:
                stm = stm.where(table.c.pixel == None)

        return stm
