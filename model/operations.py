from sqlalchemy.sql import select, and_, or_
from sqlalchemy import Table, cast, Integer, func, case
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
        table_footprint = Table(params['db'], dal.metadata, autoload=True,
                                schema=params['schema_input'])
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


class Zero_Point(IOperation):
    OPERATION = 'zero_point'

    BANDS = ['g', 'r', 'i', 'z', 'y']
    CORRECTION_TYPES = {
        "extinction_and_slr",
        "only_extinction",
        "sfd98"
        }

    def __init__(self):
        self.t_coadd = None
        self.t_zp = None
        self.columns = None

    def get_statement(self, params, sub_operations):
        if params['correction_type'] not in Zero_Point.CORRECTION_TYPES:
            raise "Correction_type unvailable."

        # load tables.
        self.t_coadd = Table(params['table_coadd_objects'], dal.metadata,
                             autoload=True,
                             schema=params['schema_input']).alias('data_set')

        self.t_zp = Table(params['table_zp'], dal.metadata, autoload=True,
                          schema=params['schema_zp']).alias('zero_point')

        columns_apply_zp = Zero_Point.columns_to_apply_zp(params['columns'])
        columns_cuts = self.get_columns_from_cuts_op(params, sub_operations)
        self.columns = columns_apply_zp | columns_cuts

        corrected_columns = self.get_columns_corrected(params, sub_operations)
        slr_columns = self.get_slr_shift_corrected(params, sub_operations)

        stm_join = self.t_coadd
        stm_join = stm_join.join(self.t_zp, self.t_zp.c.coadd_objects_id ==
                                 self.t_coadd.c.coadd_objects_id)
        all_columns = [self.t_coadd.c.coadd_objects_id] + corrected_columns + slr_columns
        stm = select(all_columns).select_from(stm_join)
        return stm

    def get_slr_shift_corrected(self, params, sub_operations):
        slr = []
        if params['add_slr_shift_columns']:
            for band in Zero_Point.BANDS:
                cur_slr = ""

                if params['correction_type'] == 'extinction_and_slr':
                    col_zp_ext = getattr(self.t_zp.c, "ext_%s" % band)
                    col_zp_minus = getattr(self.t_zp.c,
                                           "zp_minus_ext_%s" % band)
                    cur_slr = - col_zp_ext + col_zp_minus
                elif params['correction_type'] == 'only_extinction':
                    col_zp_ext = getattr(self.t_zp.c, "ext_%s" % band)
                    cur_slr = - col_zp_ext
                elif params['correction_type'] == 'sfd98':
                    col_coadd_xcorr_sfd98 = getattr(self.t_coadd.c,
                                                    'xcorr_sfd98_%s' % band)
                    cur_slr = - col_coadd_xcorr_sfd98

                slr.append((cur_slr).label('slr_shift_%s' % band))
        return slr

    def get_columns_corrected(self, params, sub_operations):
        cases = []
        for column in self.columns:
            _filter = column[-1]
            col_coadd = getattr(self.t_coadd.c, column)
            cur_else = ""

            if params['correction_type'] == 'extinction_and_slr':
                col_zp_ext = getattr(self.t_zp.c, "ext_%s" % _filter)
                col_zp_minus = getattr(self.t_zp.c,
                                       "zp_minus_ext_%s" % _filter)
                cur_else = col_coadd - col_zp_ext + col_zp_minus
            elif params['correction_type'] == 'only_extinction':
                col_zp_ext = getattr(self.t_zp.c, "ext_%s" % _filter)
                cur_else = col_coadd - col_zp_ext
            elif params['correction_type'] == 'sfd98':
                col_coadd_xcorr_sfd98 = getattr(self.t_coadd.c,
                                                'xcorr_sfd98_%s' % _filter)
                cur_else = col_coadd - col_coadd_xcorr_sfd98

            cases.append(case([(col_coadd == literal_column('99'),
                         literal_column('99')), ],
                         else_=cur_else).label(column))
        return cases

    def get_columns_from_cuts_op(self, params, sub_operations):
        columns = set()
        if params['add_cuts_columns']:
            for band in Cuts.BANDS:
                columns.add(Cuts.to_magerr_column(params['mag_type'], band))
                columns.add(Cuts.to_mag_column(params['mag_type'], band))
        return columns

    @staticmethod
    def is_zero_point_column(column):
        if 'mag_' in column:
            return True
        return False

    @staticmethod
    def columns_to_apply_zp(columns):
        columns_zp = set()
        for column in columns:
            if Zero_Point.is_zero_point_column(column):
                columns_zp.add(column)
        return columns_zp


class Cuts(IOperation):
    OPERATION = 'cuts'
    BANDS = ['g', 'r', 'i', 'z', 'y']

    MAG_TYPE = {}
    MAG_TYPE['detmodel'] = 'mag_detmodel'
    MAG_TYPE['auto'] = 'mag_auto'
    MAG_TYPE['wavgcalib'] = 'wavgcalib_mag_psf'
    MAG_TYPE['aper_4'] = 'mag_aper_4'
    MAG_TYPE['aper_8'] = 'mag_aper_8'

    MAGERR_TYPE = {}
    MAGERR_TYPE['detmodel'] = 'magerr_detmodel'
    MAGERR_TYPE['auto'] = 'magerr_auto'
    MAGERR_TYPE['wavgcalib'] = 'wavg_magerr_psf'
    MAGERR_TYPE['aper_4'] = 'magerr_aper_4'
    MAGERR_TYPE['aper_8'] = 'magerr_aper_8'

    @staticmethod
    def to_mag_column(mag_type, band):
        return ('%s_%s' % (Cuts.MAG_TYPE[mag_type], band))

    @staticmethod
    def to_magerr_column(mag_type, band):
        return ('%s_%s' % (Cuts.MAGERR_TYPE[mag_type], band))

    def get_statement(self, params, sub_operations):
        t_reduction = Table(sub_operations['reduction'].save_at(),
                            dal.metadata, autoload=True,
                            schema=dal.schema_output)
        t_coadd = Table(params['table_coadd_objects'], dal.metadata,
                        autoload=True,
                        schema=params['schema_input']).alias('data_set')

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
                            t_coadd.c.nepochs_g > literal_column('0'),
                            t_coadd.c.magerr_auto_g > literal_column('0.05'),
                            t_coadd.c.mag_model_i - t_coadd.c.mag_auto_i >
                            literal_column('-0.4')
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
                                 t_coadd.c.alphawin_j2000_i) <
                        literal_column('0.0003'),
                        func.abs(t_coadd.c.deltawin_j2000_g -
                                 t_coadd.c.deltawin_j2000_i) <
                        literal_column('0.0003')
                    ),
                    or_(
                        t_coadd.c.magerr_auto_g > literal_column('0.05')
                    )
                ))

        t_cur = t_coadd
        if 'zero_point' in sub_operations:
            t_cur = Table(sub_operations['zero_point'].save_at(), dal.metadata,
                          autoload=True, schema=dal.schema_output).alias('zero_point')
            stm_join = stm_join.join(t_cur, t_reduction.c.coadd_objects_id ==
                                     t_cur.c.coadd_objects_id)

        # signal to noise cuts
        if 'sn_cuts' in params:
            tmp = []
            for element in params['sn_cuts'].items():
                band, value = element
                db_col = Cuts.to_magerr_column(params['mag_type'], band)
                col = getattr(t_cur.c, db_col)
                tmp.append(and_(
                        col > literal_column('0'),
                        literal_column('1.086')/col >
                        literal_column(str(value))
                    ))
            _where.append(and_(*tmp))

        # magnitude limit
        if 'magnitude_limit' in params:
            tmp = []
            for element in params['magnitude_limit'].items():
                band, value = element
                db_col = Cuts.to_mag_column(params['mag_type'], band)
                col = getattr(t_cur.c, db_col)
                tmp.append(col < literal_column(str(value)))
            _where.append(and_(*tmp))

        # bright magnitude limit
        if 'bright_magnitude' in params:
            tmp = []
            for element in params['bright_magnitude'].items():
                band, value = element
                db_col = Cuts.to_mag_column(params['mag_type'], band)
                col = getattr(t_cur.c, db_col)
                tmp.append(col > literal_column(str(value)))
            _where.append(and_(*tmp))

        # color cuts
        if 'color_cuts' in params:
            tmp = []
            for element in params['color_cuts'].items():
                band, value = element
                db_col_max = Cuts.to_mag_column(params['mag_type'], band[0])
                db_col_min = Cuts.to_mag_column(params['mag_type'], band[1])
                col_max = getattr(t_cur.c, db_col_max)
                col_min = getattr(t_cur.c, db_col_min)
                tmp.append(between(literal_column(str(col_max - col_min)),
                                   literal_column(str(value[0])),
                                   literal_column(str(value[1]))))
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
        _where.append(alias_table.c.hole_bitmask != literal_column('1'))

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
            _where.append(and_(table.c.z_best >
                               literal_column(str(params['zmin'])),
                               table.c.z_best <
                               literal_column(str(params['zmax']))))

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
