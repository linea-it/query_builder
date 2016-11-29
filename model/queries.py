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
        self._table_to_load = 'map_table'

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
