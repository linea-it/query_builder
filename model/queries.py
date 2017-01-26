from sqlalchemy.sql import select, and_, or_
from sqlalchemy import Table, cast, Integer, func
from sqlalchemy.sql.expression import literal_column, between

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
        elif operation_type == SgSeparation.QUERY:
            query = SgSeparation()
        elif operation_type == PhotoZ.QUERY:
            query = PhotoZ()
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
    BANDS = ['g', 'r', 'i', 'z', 'y']

    def get_statement(self, params, sub_operations):
        key, values = list(params.items())[0]
        sub_op_names = list(values['sub_op'].keys())

        schema = values['schema'] if 'schema' in values else None
        footprint_op = sub_operations['sub_op']['footprint']

        # load tables.
        t_footprint = Table(footprint_op.save_at(), dal.metadata,
                            autoload=True)
        t_coadd = Table(values['table_coadd_objects'],
                        dal.metadata, autoload=True, schema=schema)
        t_objects_ring = Table(values['table_coadd_objects_ring'],
                               dal.metadata, autoload=True, schema=schema)

        # join statement
        stm_join = t_footprint
        stm_join = stm_join.join(t_objects_ring, t_footprint.c.pixel ==
                                 t_objects_ring.c.pixel)
        stm_join = stm_join.join(t_coadd, t_objects_ring.c.coadd_objects_id ==
                                 t_coadd.c.coadd_objects_id)

        _where = []

        # bitmask
        alias_table = None
        if 'mangle_bitmask' in values:
            t_coadd_molygon = Table(values['table_coadd_objects_molygon'],
                                    dal.metadata, autoload=True, schema=schema)
            t_molygon = Table(values['table_molygon'],
                              dal.metadata, autoload=True, schema=schema)

            stm_join = stm_join.join(t_coadd_molygon,
                                     t_coadd.c.coadd_objects_id ==
                                     t_coadd_molygon.c.coadd_objects_id)

            for band in values['mangle_bitmask']:
                # give the str column and retrieve the attribute.
                alias_table = t_molygon.alias('molygon_%s' % band)
                col = getattr(t_coadd_molygon.c, 'molygon_id_%s' % band)
                stm_join = stm_join.join(alias_table,
                                         col == alias_table.c.id)
            _where.append(alias_table.c.hole_bitmask != 1)

        # cuts involving only coadd_objects_columns
        # sextractor flags
        if 'sextractor_bands' in values and\
           'sextractor_flags' in values:
            # combine_flags
            queries = []
            sum_flags = sum(values['sextractor_flags'])
            for band in values['sextractor_bands']:
                query = []
                col = getattr(t_coadd.c, 'flags_%s' % band)
                if 0 in values['sextractor_flags']:
                    query.append(col == literal_column('0'))
                if sum_flags > 0:
                    and_op = sql_operations.BitwiseAnd(
                                   col,
                                   literal_column(str(sum_flags)))
                    query.append((and_op) > literal_column('0'))
                queries.append(or_(*query))
            _where.append(and_(*queries))

        # bbj
        if 'remove_bbj' in values['additional_cuts']:
            _where.append(or_(
                            t_coadd.c.nepochs_g > 0,
                            t_coadd.c.magerr_auto_g > 0.05,
                            t_coadd.c.mag_model_i - t_coadd.c.mag_auto_i > -0.4
                            ))

        # niter model
        if 'niter_model' in values['additional_cuts']:
            tmp = []
            for band in ObjectSelection.BANDS:
                col = getattr(t_coadd.c, 'niter_model_%s' % band)
                tmp.append(col > literal_column('0'))
            _where.append(and_(*tmp))

        # spreaderr model
        if 'spreaderr_model' in values['additional_cuts']:
            tmp = []
            for band in ObjectSelection.BANDS:
                col = getattr(t_coadd.c, 'spreaderr_model_%s' % band)
                tmp.append(col > literal_column('0'))
            _where.append(and_(*tmp))

        # bad astronomic color
        if 'bad_astronomic_colors' in values['additional_cuts']:
            _where.append(and_(
                    and_(
                        func.abs(t_coadd.c.alphawin_j2000_g -
                                 t_coadd.c.alphawin_j2000_i) < 0.0003,
                        func.abs(t_coadd.c.deltawin_j2000_g -
                                 t_coadd.c.deltawin_j2000_i) < 0.0003
                    ),
                    or_(
                        t_coadd.c.magerr_auto_g > 0.05
                    )
                ))

        # REVIEW: zero_point is not beeing applied. mag_auto is hardcoded.
        # signal to noise cuts
        if 'sn_cuts' in values:
            tmp = []
            for element in values['sn_cuts'].items():
                band, value = element
                col = getattr(t_coadd.c, 'magerr_auto_%s' % band)
                tmp.append(and_(
                        col > literal_column('0'),
                        1.086/col > literal_column(str(value))
                    ))
            _where.append(and_(*tmp))

        # magnitude limit
        if 'magnitude_limit' in values:
            tmp = []
            for element in values['magnitude_limit'].items():
                band, value = element
                col = getattr(t_coadd.c, 'mag_auto_%s' % band)
                tmp.append(col < literal_column(str(value)))
            _where.append(and_(*tmp))

        # bright magnitude limit
        if 'bright_magnitude' in values:
            tmp = []
            for element in values['bright_magnitude'].items():
                band, value = element
                col = getattr(t_coadd.c, 'mag_auto_%s' % band)
                tmp.append(col > literal_column(str(value)))
            _where.append(and_(*tmp))

        # color cuts
        if 'color_cuts' in values:
            tmp = []
            for element in values['color_cuts'].items():
                band, value = element
                print(band)
                print(value)
                col_max = getattr(t_coadd.c, 'mag_auto_%s' % band[0])
                col_min = getattr(t_coadd.c, 'mag_auto_%s' % band[1])
                tmp.append(between(col_max - col_min, value[0], value[1]))
                print(str(between(col_max - col_min, value[0], value[1])))
            _where.append(and_(*tmp))

        stm = select([t_coadd.c.coadd_objects_id]).\
            select_from(stm_join).where(and_(*_where))

        print(str(stm))
        return stm


class SgSeparation(IQuery):
    QUERY = 'sg_separation'

    def get_statement(self, params, sub_operations):
        key, values = list(params.items())[0]

        schema = values['schema'] if 'schema' in values else None
        obj_selection_op = sub_operations['sub_op']['object_selection']

        # load tables.
        t_obj_selection = Table(obj_selection_op.save_at(), dal.metadata,
                                autoload=True)
        t_sg = []
        for table in values['tables_sg']:
            t_sg.append(Table(table, dal.metadata, autoload=True,
                              schema=schema))

        _where = []
        # join statement
        stm_join = t_obj_selection
        for table in t_sg:
            stm_join = stm_join.join(
                table, t_obj_selection.c.coadd_objects_id ==
                table.c.coadd_objects_id)
            col = getattr(table.c, '%s' % values['ref_band'])
            _where.append(col == literal_column('0'))

        stm = select([t_obj_selection.c.coadd_objects_id]).\
            select_from(stm_join).where(and_(*_where))

        print(stm)
        return stm


class PhotoZ(IQuery):
    QUERY = 'photoz'

    def get_statement(self, params, sub_operations):
        key, values = list(params.items())[0]

        schema = values['schema'] if 'schema' in values else None
        sub_op = sub_operations['sub_op'].values()[0]

        # load tables.
        t_sub_op = Table(sub_op.save_at(), dal.metadata, autoload=True)
        t_pz = []
        for table in values['tables_zp']:
            t_pz.append(Table(table, dal.metadata, autoload=True,
                              schema=schema))

        _where = []
        # join statement
        stm_join = t_sub_op
        for table in t_pz:
            stm_join = stm_join.join(
                table, t_sub_op.c.coadd_objects_id ==
                table.c.coadd_objects_id)
            _where.append(and_(table.c.z_best > values['zmin'],
                               table.c.z_best < values['zmax']))

        stm = select([t_sub_op.c.coadd_objects_id]).\
            select_from(stm_join).where(and_(*_where))

        print(stm)
        return stm
