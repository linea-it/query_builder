import json
from sqlalchemy import (MetaData, create_engine, Table)
import settings


class DataAccessLayer():
    def __init__(self):
        self.engine = None
        self.metadata = MetaData()
        self.tables = {}

    def db_init(self, con_string):
        self.engine = create_engine(con_string)
        with self.engine.connect() as con:
            self.metadata = MetaData(self.engine)

dal = DataAccessLayer()


def str_connection():
    db = settings.DATABASES[settings.DATABASE]
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
