from sqlalchemy.sql import select
from sqlalchemy import Table
from sqlalchemy import (MetaData, create_engine)


"""
    Access to the database and metadata through a singleton.
"""


class DataAccessLayer():
    def __init__(self):
        self.engine = None
        self.metadata = MetaData()
        self.schema_output = None

    def db_init(self, con_string, schema_output=None):
        self.schema_output = schema_output
        self.engine = create_engine(con_string)
        with self.engine.connect() as con:
            self.metadata = MetaData(self.engine)

    @staticmethod
    def str_connection(db):
        """
        Builds string connection.
        """
        str_con = db['dialect']
        if 'driver' in db:
            str_con += '+' + db['driver']
        str_con += '://' + db['username'] + ":" + db['password'] + '@' +\
                   db['host'] + ':' + db['port'] + '/'
        if 'database' in db:
            str_con += db['database']
        else:
            db['database'] = None
        return str_con


def create_columns_sql_format(table_obj, columns):
    t_columns = table_obj
    if columns is not None:
        t_columns = list()
        for col in columns:
            t_columns.append(getattr(table_obj.c, col))
    return t_columns


def select_columns(table_name, columns=None):
    with dal.engine.connect() as con:
        table = Table(table_name, dal.metadata,
                      autoload=True, schema=dal.schema_output)
        cols = create_columns_sql_format(table, columns)
        stm = select(cols).select_from(table)
        print(str(stm))
        result = con.execute(stm)
        return result.fetchall()


dal = DataAccessLayer()
