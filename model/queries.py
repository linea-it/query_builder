from sqlalchemy.sql import select
from sqlalchemy import Table, cast, Integer
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
        elif operation_type == ObjectSelection.QUERY:
            query = ObjectSelection()
        else:
            raise "This query is not implemented."

        return query


class GreatEqual(IQuery):
    QUERY = "great_equal"

    def get_statement(self, params, sub_operations):
        key, value = list(params.items())[0]

        schema = value['schema'] if 'schema' in value else None
        table = Table(value['db'], dal.metadata, autoload=True,
                      schema=schema)
        stm = select(
          [table]).where(table.c.signal >= literal_column(value['value']))
        return stm


class CombinedMaps(IQuery):
    QUERY = 'join'

    def get_statement(self, params, sub_operations):
        sub_op_names = list(params[list(params.keys())[0]]['sub_op'].keys())

        # load tables.
        key, value = list(params.items())[0]
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

        schema = value['schema'] if 'schema' in value else None
        table = Table(value['db'], dal.metadata, autoload=True,
                      schema=schema)
        stm = select([table]).where(sql_operations.BitwiseAnd(
                                    cast(table.c.signal, Integer),
                                    literal_column(value['value'])) >
                                    literal_column('0'))
        return stm


class Footprint(IQuery):
    QUERY = 'footprint'

    def get_statement(self, params, sub_operations):
        inner_join = ["exposure_time", "depth_map", "mangle_map"]
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
        key, value = list(params.items())[0]

        schema = value['schema'] if 'schema' in value else None
        table_footprint = Table(list(params.values())[0]['db'], dal.metadata,
                                autoload=True, schema=schema)
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


class ObjectSelection(IQuery):
    QUERY = 'object_selection'

    def get_statement(self, params, sub_operations):
        key, value = list(params.items())[0]
        sub_op_names = list(value['sub_op'].keys())

        schema = value['schema'] if 'schema' in value else None
        footprint_op = sub_operations['sub_op']['footprint']

        # load tables.
        t_footprint = Table(footprint_op.save_at(), dal.metadata,
                            autoload=True)
        t_coadd = Table(value['table_coadd_objects'],
                        dal.metadata, autoload=True, schema=schema)
        t_objects_ring = Table(value['table_coadd_objects_ring'],
                               dal.metadata, autoload=True, schema=schema)

        # join statement
        stm_join = t_footprint
        stm_join = stm_join.join(t_objects_ring, t_footprint.c.pixel ==
                                 t_objects_ring.c.pixel)
        stm_join = stm_join.join(t_coadd, t_objects_ring.c.coadd_objects_id ==
                                 t_coadd.c.coadd_objects_id)

        # bitmask
        alias_table = None
        if 'mangle_bitmask' in value:
            t_coadd_molygon = Table(value['table_coadd_objects_molygon'],
                                    dal.metadata, autoload=True, schema=schema)
            t_molygon = Table(value['table_molygon'],
                              dal.metadata, autoload=True, schema=schema)

            stm_join = stm_join.join(t_coadd_molygon,
                                     t_coadd.c.coadd_objects_id ==
                                     t_coadd_molygon.c.coadd_objects_id)

            for band in value['mangle_bitmask']:
                # give the str column and retrieve the attribute.
                alias_table = t_molygon.alias('molygon_%s' % band)
                col = getattr(t_coadd_molygon.c, 'molygon_id_%s' % band)
                stm_join = stm_join.join(alias_table,
                                         col == alias_table.c.id)

        stm = select([t_coadd.c.coadd_objects_id]).select_from(stm_join)

        if 'mangle_bitmask' in value:
            stm = stm.where(alias_table.c.hole_bitmask != 1)

        return stm
