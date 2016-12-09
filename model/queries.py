from sqlalchemy.sql import select
from sqlalchemy import (create_engine, Table, MetaData)
from sqlalchemy.sql.expression import literal_column

from inout import input_settings
from model import sql_operations as op
from utils.db import dal


class Operation():
    def __init__(self):
        self._stm = self.statement()

    def __str__(self):
        return (str(self._stm))

    def create(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.CreateTableAs(self.save_at(), self._stm))

    def delete(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.DropTable(self.save_at()))

    def save_at(self):
        raise NotImplementedError("Implement this method")

    def statement(self):
        raise NotImplementedError("Implement this method")


class ExposureTime(Operation):
    OP = 'exposure_time'

    def __init__(self, element):
        self.element = element

        Operation.__init__(self)

    def statement(self):
        table = dal.tables[ExposureTime.OP]
        stm = select(
          [
            table.c.pixel,
            table.c.signal,
            table.c.ra,
            table.c.dec
          ]).where(table.c.signal >= literal_column(self.element['value']))
        return stm

    def save_at(self):
        return ExposureTime.OP + "_" + input_settings.PROCESS['id']


class BadRegions(Operation):
    OP = 'bad_regions'

    def __init__(self):
        Operation.__init__(self)

    def statement(self):
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

    def save_at(self):
        return BadRegions.OP + "_" + input_settings.PROCESS['id']
