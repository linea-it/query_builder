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

    def db_init(self, con_string):
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
        print(str_con)
        return str_con

dal = DataAccessLayer()
