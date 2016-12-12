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
        self.set_operation(operation_type)

    def set_operation(self, operation_type):
        # register operations
        operations = {}
        operations[ExposureTime.OP] = ExposureTime()
        operations[BadRegions.OP] = BadRegions()

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
        return self.operation_type + "_" + input_settings.PROCESS['id']


class Statement():
    def get_statement(self):
        raise NotImplementedError("Implement this method")


class ExposureTime(Statement):
    OP = "exposure_time"

    def get_statement(self):
        element = {'band': 'g', 'value': '0.55', 'name': 'exposure_time_i'}
        table = dal.tables[ExposureTime.OP]
        stm = select(
          [
            table.c.pixel,
            table.c.signal,
            table.c.ra,
            table.c.dec
          ]).where(table.c.signal >= literal_column(element['value']))
        return stm


class BadRegions(Statement):
    OP = 'bad_regions'

    def get_statement(self):
        mask = 0
        for element in input_settings.OPERATIONS['bad_regions']:
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
