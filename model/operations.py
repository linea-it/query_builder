from sqlalchemy.sql import select, and_, or_
from sqlalchemy import Table, cast, Integer, func
from sqlalchemy.sql.expression import literal_column, between

from utils.db import dal

from model import sql_operations


"""
An operation represents a query that is built based on the input configuration
-params- and optionally, it can depend on intermediate tables -sub_operations-.
This way, a single operation can be used to compose many queries.

Basically, when a new operation must be written, we basically heritage the
IOperation class and override the method get_statement.
"""


class IOperation():
    """
    Abstract class that defines the interface to create new operations.
    """
    def get_statement(self, params, sub_operations):
        """
        This method defines the operation. *args:
        params - a dictionary that has specific information about the
        operation.
        sub_operations - It has a list of operations in which this new
        operation depends.
        It must return a SQLAlchemy select statement.
        """
        raise NotImplementedError("Implement this method")


class GreatEqual(IOperation):
    OPERATION = "great_equal"

    def get_statement(self, params, sub_operations):
        table = Table(params['db'], dal.metadata, autoload=True,
                      schema=params['schema_input'])
        stm = select(
          [table]).where(table.c.signal >= literal_column(params['value']))
        return stm


class CombinedMaps(IOperation):
    OPERATION = 'join'

    def get_statement(self, params, sub_operations):
        # load tables.
        sub_tables = []
        for table in sub_operations.values():
            sub_tables.append(Table(table.save_at(), dal.metadata,
                                    schema=dal.schema_output, autoload=True))

        # join statement
        stm = select([sub_tables[0]])
        stm_join = sub_tables[0]
        for i in range(1, len(sub_tables)):
            stm_join = stm_join.join(sub_tables[i], sub_tables[i-1].c.pixel ==
                                     sub_tables[i].c.pixel)
        stm = stm.select_from(stm_join)
        return stm


class BadRegions(IOperation):
    OPERATION = "bad_regions"

    def get_statement(self, params, sub_operations):
        table = Table(params['db'], dal.metadata, autoload=True,
                      schema=params['schema_input'])
        stm = select([table]).where(sql_operations.BitwiseAnd(
                                    cast(table.c.signal, Integer),
                                    literal_column(params['value'])) >
                                    literal_column('0'))
        return stm


class Footprint(IOperation):
    OPERATION = 'footprint'

    def get_statement(self, params, sub_operations):
        inner_join = ["exposure_time", "depth_map", "mangle_map"]
        left_join = ["bad_regions"]

        inner_join_ops = []
        left_join_ops = []

        # divide operations accordingly
        if sub_operations:
            for k, v in list(sub_operations.items()):
                if k in inner_join:
                    inner_join_ops.append(v)
                elif k in left_join:
                    left_join_ops.append(v)
                else:
                    raise("operations does not exist.")

        # load tables.
        # review from data.
        table_footprint = Table(params['db'], dal.metadata,
                                autoload=True, schema=params['schema_input'])
        sub_tables_inner = []
        for table in inner_join_ops:
            sub_tables_inner.append(Table(table.save_at(), dal.metadata,
                                    autoload=True, schema=dal.schema_output))
        sub_tables_left = []
        for table in left_join_ops:
            sub_tables_left.append(Table(table.save_at(), dal.metadata,
                                   autoload=True, schema=dal.schema_output))

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


class Reduction(IOperation):
    OPERATION = 'reduction'

    def get_statement(self, params, sub_operations):
        # load tables.
        t_footprint = Table(sub_operations['footprint'].save_at(),
                            dal.metadata, autoload=True,
                            schema=dal.schema_output)
        t_objects_ring = Table(params['table_coadd_objects_ring'],
                               dal.metadata, autoload=True,
                               schema=params['schema_input'])

        # join statement
        stm_join = t_footprint
        stm_join = stm_join.join(t_objects_ring, t_footprint.c.pixel ==
                                 t_objects_ring.c.pixel)
        stm = select([t_objects_ring.c.coadd_objects_id]).\
            select_from(stm_join)
        return stm


class Cuts(IOperation):
    OPERATION = 'cuts'

    BANDS = ['g', 'r', 'i', 'z', 'y']

    def get_statement(self, params, sub_operations):
        t_reduction = Table(sub_operations['reduction'].save_at(),
                            dal.metadata, autoload=True,
                            schema=dal.schema_output)
        t_coadd = Table(params['table_coadd_objects'], dal.metadata,
                        autoload=True, schema=params['schema_input'])

        # join statement
        stm_join = t_reduction
        stm_join = stm_join.join(t_coadd, t_reduction.c.coadd_objects_id ==
                                 t_coadd.c.coadd_objects_id)

        _where = []

        # cuts involving only coadd_objects_columns
        # sextractor flags
        if 'sextractor_bands' in params and\
           'sextractor_flags' in params:
            # combine_flags
            queries = []
            sum_flags = sum(params['sextractor_flags'])
            for band in params['sextractor_bands']:
                query = []
                col = getattr(t_coadd.c, 'flags_%s' % band)
                if 0 in params['sextractor_flags']:
                    query.append(col == literal_column('0'))
                if sum_flags > 0:
                    and_op = sql_operations.BitwiseAnd(
                                   col,
                                   literal_column(str(sum_flags)))
                    query.append((and_op) > literal_column('0'))
                queries.append(or_(*query))
            _where.append(and_(*queries))

        # bbj
        if 'remove_bbj' in params['additional_cuts']:
            _where.append(or_(
                            t_coadd.c.nepochs_g > 0,
                            t_coadd.c.magerr_auto_g > 0.05,
                            t_coadd.c.mag_model_i - t_coadd.c.mag_auto_i > -0.4
                            ))

        # niter model
        if 'niter_model' in params['additional_cuts']:
            tmp = []
            for band in ObjectSelection.BANDS:
                col = getattr(t_coadd.c, 'niter_model_%s' % band)
                tmp.append(col > literal_column('0'))
            _where.append(and_(*tmp))

        # spreaderr model
        if 'spreaderr_model' in params['additional_cuts']:
            tmp = []
            for band in Cuts.BANDS:
                col = getattr(t_coadd.c, 'spreaderr_model_%s' % band)
                tmp.append(col > literal_column('0'))
            _where.append(and_(*tmp))

        # bad astronomic color
        if 'bad_astronomic_colors' in params['additional_cuts']:
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
        if 'sn_cuts' in params:
            tmp = []
            for element in params['sn_cuts'].items():
                band, value = element
                col = getattr(t_coadd.c, 'magerr_auto_%s' % band)
                tmp.append(and_(
                        col > literal_column('0'),
                        1.086/col > literal_column(str(value))
                    ))
            _where.append(and_(*tmp))

        # magnitude limit
        if 'magnitude_limit' in params:
            tmp = []
            for element in params['magnitude_limit'].items():
                band, value = element
                col = getattr(t_coadd.c, 'mag_auto_%s' % band)
                tmp.append(col < literal_column(str(value)))
            _where.append(and_(*tmp))

        # bright magnitude limit
        if 'bright_magnitude' in params:
            tmp = []
            for element in params['bright_magnitude'].items():
                band, value = element
                col = getattr(t_coadd.c, 'mag_auto_%s' % band)
                tmp.append(col > literal_column(str(value)))
            _where.append(and_(*tmp))

        # color cuts
        if 'color_cuts' in params:
            tmp = []
            for element in params['color_cuts'].items():
                band, value = element
                col_max = getattr(t_coadd.c, 'mag_auto_%s' % band[0])
                col_min = getattr(t_coadd.c, 'mag_auto_%s' % band[1])
                tmp.append(between(col_max - col_min, value[0], value[1]))
            _where.append(and_(*tmp))

        stm = select([t_coadd.c.coadd_objects_id]).\
            select_from(stm_join).where(and_(*_where))

        return stm


class Bitmask(IOperation):
    OPERATION = 'bitmask'

    def get_statement(self, params, sub_operations):
        sub_op = list(sub_operations.values())[0]

        # load tables.
        t_sub_op = Table(sub_op.save_at(), dal.metadata, autoload=True,
                         schema=dal.schema_output)
        _where = []

        # bitmask
        alias_table = None
        t_coadd_molygon = Table(params['table_coadd_objects_molygon'],
                                dal.metadata, autoload=True,
                                schema=params['schema_input'])
        t_molygon = Table(params['table_molygon'], dal.metadata,
                          autoload=True, schema=params['schema_input'])

        stm_join = t_sub_op
        stm_join = stm_join.join(t_coadd_molygon,
                                 t_sub_op.c.coadd_objects_id ==
                                 t_coadd_molygon.c.coadd_objects_id)

        for band in params['mangle_bitmask']:
            # give the str column and retrieve the attribute.
            alias_table = t_molygon.alias('molygon_%s' % band)
            col = getattr(t_coadd_molygon.c, 'molygon_id_%s' % band)
            stm_join = stm_join.join(alias_table,
                                     col == alias_table.c.id)
        _where.append(alias_table.c.hole_bitmask != 1)

        stm = select([t_sub_op.c.coadd_objects_id]).\
            select_from(stm_join).where(and_(*_where))

        return stm


class ObjectSelection(IOperation):
    OPERATION = 'object_selection'

    def get_statement(self, params, sub_operations):
        sub_op = list(sub_operations.values())[0]

        # load tables.
        t_sub_op = Table(sub_op.save_at(), dal.metadata, autoload=True,
                         schema=dal.schema_output)

        stm = select([t_sub_op.c.coadd_objects_id])
        return stm


class SgSeparation(IOperation):
    OPERATION = 'sg_separation'

    def get_statement(self, params, sub_operations):
        # load tables.
        t_obj_selection = Table(sub_operations['object_selection'].save_at(),
                                dal.metadata, autoload=True,
                                schema=dal.schema_output)
        t_sg = []
        for table in params['tables_sg']:
            t_sg.append(Table(table, dal.metadata, autoload=True,
                              schema=params['schema_input']))

        _where = []
        # join statement
        stm_join = t_obj_selection
        for table in t_sg:
            stm_join = stm_join.join(
                table, t_obj_selection.c.coadd_objects_id ==
                table.c.coadd_objects_id)
            col = getattr(table.c, '%s' % params['ref_band'])
            _where.append(col == literal_column('0'))

        stm = select([t_obj_selection.c.coadd_objects_id]).\
            select_from(stm_join).where(and_(*_where))

        return stm


class PhotoZ(IOperation):
    OPERATION = 'photoz'

    def get_statement(self, params, sub_operations):
        sub_op = list(sub_operations.values())[0]

        # load tables.
        t_sub_op = Table(sub_op.save_at(), dal.metadata, autoload=True,
                         schema=dal.schema_output)
        t_pz = []
        for table in params['tables_zp']:
            t_pz.append(Table(table, dal.metadata, autoload=True,
                              schema=params['schema_input']))

        _where = []
        # join statement
        stm_join = t_sub_op
        for table in t_pz:
            stm_join = stm_join.join(
                table, t_sub_op.c.coadd_objects_id ==
                table.c.coadd_objects_id)
            _where.append(and_(table.c.z_best > params['zmin'],
                               table.c.z_best < params['zmax']))

        stm = select([t_sub_op.c.coadd_objects_id]).\
            select_from(stm_join).where(and_(*_where))

        return stm


class GalaxyProperties(IOperation):
    OPERATION = 'galaxy_properties'

    def get_statement(self, params, sub_operations):
        sub_op = list(sub_operations.values())[0]

        # load tables.
        t_sub_op = Table(sub_op.save_at(), dal.metadata, autoload=True,
                         schema=dal.schema_output)
        t_gp = []
        for table in params['tables_gp']:
            t_gp.append(Table(table, dal.metadata, autoload=True,
                              schema=params['schema_input']))

        # join statement
        stm_join = t_sub_op
        for table in t_gp:
            stm_join = stm_join.join(
                table, t_sub_op.c.coadd_objects_id == table.c.coadd_objects_id)

        stm = select([t_sub_op.c.coadd_objects_id]).\
            select_from(stm_join)

        return stm
