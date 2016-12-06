import json
from sqlalchemy import (MetaData, create_engine, Table)
import settings


class DataAccessLayer():
    engine = None
    metadata = MetaData()
    tables = {}

    def db_init(self, con_string):
        self.engine = create_engine(con_string)
        self.metadata.create_all(self.engine)

    def load_tables(self):
        data = {}
        with open(settings.DB_TABLES) as data_file:
            data = json.load(data_file)

        with self.engine.connect() as con:
            for operation in data:
                self.tables[operation] = Table(data[operation],
                                               self.metadata, autoload=True)

dal = DataAccessLayer()


def str_connection():
    db = settings.DATABASES[settings.DATABASE]
    str_con = db['dialect']
    if 'driver' in db:
        str_con += '+' + db['driver']
    str_con += '://' + db['username'] + ":" + db['password'] + '@' +\
               db['host'] + ':' + db['port'] + '/' + db['database']
    return str_con
