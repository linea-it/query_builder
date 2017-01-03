from sqlalchemy.sql import select
from sqlalchemy import (create_engine, Table, MetaData)
from sqlalchemy.sql.expression import literal_column

from inout import input_settings
from model import sql_operations as op
from utils.db import dal


class Operation():
    def __init__(self, operation_type):
        self.operation_type = operation_type

        self._stm = None
        self._set_operation(operation_type)

    def _set_operation(self, operation_type):
        # register operations
        operations = {}
        operations[ExposureTime.OP] = ExposureTime()
        operations[BadRegions.OP] = BadRegions()
        operations[CombinedMaps.OP] = CombinedMaps()

        if operation_type not in operations:
            raise "This operations is not registered"
        else:
            self._stm = operations[operation_type]

    def __str__(self):
        return (str(self._stm.get_statement()))

    def create(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.CreateTableAs(self.save_at(),
                        self._stm.get_statement()))

    def delete(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.DropTable(self.save_at()))

    def save_at(self):
        return self.operation_name() + "_" + input_settings.PROCESS['id']


class Statement():
    def get_statement(self):
        raise NotImplementedError("Implement this method")

    def operation_name(self):
        raise NotImplementedError("Implement this method")


class GreatEqual(Statement):
    OP = "great_equal"

    def __init__(self, param):
        self.param = param
        # checks ?

    def get_statement(self):
        table = dal.tables[ExposureTime.OP]
        stm = select(
          [
            table.c.pixel,
            table.c.signal,
            table.c.ra,
            table.c.dec
          ]).where(table.c.signal >= literal_column(self.param['value']))
        return stm

    def operation_name(self):
        return self.param('name')



class BadRegions(Statement):
    OP = 'bad_regions'

    def __init__(self, param):
        self.param = param

    def get_statement(self):
        mask = 0
        for element in self.param:
            mask += int(element['value'])

        print ('Mask = %d' % mask)

        table = dal.tables[BadRegions.OP]
        stm = select(
          [
            table.c.pixel,
            table.c.signal,
            table.c.ra,
            table.c.dec
          ]).where(op.BitwiseAnd(table.c.signal,
                   literal_column(str(mask))) > literal_column('0'))
        return stm

    def operation_name(self):
        return BadRegions.OP


class SystematicMap(Statement):
    OP = "systematic_map"

    def __init__(self):
        self._maps = []
        self._tables_name = []

        if BadRegions.OP in self._input:
            bad_regions = Operation(BadRegions.OP)
            self._maps.append(bad_regions)
            self._unite.append(bad_regions.save_at())

        if RadialMap.OP in self._input:
            radial_map = Operation(RadialMap.OP)
            self._maps.append(radial_map)
            self._unite.append(radial_map.save_at())

        if CombinedMaps.OP in self._input:
            combined_maps = Operation(CombinedMaps.OP)
            self._maps.extend(combined_maps.all_maps())
            self._intersect.append(combined_maps.tables_to_intersect())

    def get_statement(self):
        table = dal.tables[ExposureTime.OP]
        stm = select(
          [
            table.c.pixel,
            table.c.signal,
            table.c.ra,
            table.c.dec
          ]).where(table.c.signal >= literal_column(self.element['value']))
        return stm

    def operation_name(self):
        return self.element('name')


class CombinedMaps(Statement):
    OP = 'combined_maps'

    def __init__(self):
        return

    def get_statement(self):
        combined_maps = input_settings.OPERATIONS['combined_maps']

        for sys_maps in combined_maps:
            for element in combined_maps[sys_maps]:
                operation = Operation('great_equal')
                operation.create()


class FootprintMap(Statement):
    OP = 'footprint_map'

    def __init__(self):
        self._input = input_settings.OPERATIONS

        self._maps = []
        self._unite = []
        self._intersect = []

        if BadRegions.OP in self._input:
            bad_regions = Operation(BadRegions.OP)
            self._maps.append(bad_regions)
            self._unite.append(bad_regions.save_at())

        if RadialMap.OP in self._input:
            radial_map = Operation(RadialMap.OP)
            self._maps.append(radial_map)
            self._unite.append(radial_map.save_at())

        if CombinedMaps.OP in self._input:
            combined_maps = Operation(CombinedMaps.OP)
            self._maps.extend(combined_maps.all_maps())
            self._intersect.append(combined_maps.tables_to_intersect())

    def get_statement(self):
        return

    def operation_name(self):
        return FootprintMap.OP

    def all_maps(self):
        return self._maps


class QueryBuilder(Statement):
    def __init__(self):

        self._footprint = Operation(FootprintMap.OP)
        # self._catalog = Operation(Catalog.OP)

    def all_operations(self):
        return return self._footprint.all_maps()
