from sqlalchemy.sql import select
from sqlalchemy import (create_engine, Table, MetaData)
from sqlalchemy.sql.expression import literal_column

from utils import db_connection
from inout import input_settings
from model import sql_operations as op


class ExposureTime():
    OP = 'exposure_time'

    def __init__(self, element):
        # load related tables and verify your existence.
        self._table_to_load = 'systematic_map'

        eng = create_engine(db_connection.DbConnection().str_connection())
        with eng.connect() as con:
            meta = MetaData(eng)
            table = Table(self._table_to_load, meta, autoload=True)

            self._stm = select(
              [
                table.c.pixel,
                table.c.signal,
                table.c.ra,
                table.c.dec
              ]).where(table.c.signal >= literal_column(element['value']))

    def query(self):
        return (str(self._stm))

    def create(self):
        eng = create_engine(db_connection.DbConnection().str_connection())
        with eng.connect() as con:
            con.execute("commit")
            con.execute(op.CreateTableAs(self.save_at(), self._stm))

    def save_at(self):
        return ExposureTime.OP + "_" + input_settings.PROCESS['id']


class BadRegions():
    OP = 'bad_regions'

    def __init__(self):
        # load related tables and verify your existence.
        self._table_to_load = 'bad_regions_map'

        mask = 0
        for element in input_settings.OPERATIONS['bad_regions']:
            mask += int(element['value'])

        print ('Mask = %d' % mask)

        eng = create_engine(db_connection.DbConnection().str_connection())
        with eng.connect() as con:
            meta = MetaData(eng)
            table = Table(self._table_to_load, meta, autoload=True)

            self._stm = select(
              [
                table.c.pixel,
                table.c.signal,
                table.c.ra,
                table.c.dec
              ]).where(op.BitwiseAnd(table.c.signal, mask) > 0)

    def query(self):
        return (str(self._stm))

    def create(self):
        eng = create_engine(db_connection.DbConnection().str_connection())
        with eng.connect() as con:
            con.execute("commit")
            con.execute(op.CreateTableAs(self.save_at(), self._stm))

    def save_at(self):
        return BadRegions.OP + "_" + input_settings.PROCESS['id']
