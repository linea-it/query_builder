import json
from sqlalchemy import (MetaData, create_engine, Table)
import settings


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

dal = DataAccessLayer()
