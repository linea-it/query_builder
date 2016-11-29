import sys

from sqlalchemy import create_engine
import settings


class DbConnection():
    def __init__(self):
        self._dict_con = self._load_settings()

    def _load_settings(self):
        """ Expected format
        }
            'dialect': 'postgresql',
            'driver': 'psycopg2',
            'username': 'lucasdpn',
            'password': 'tet123456',
            'host': 'localhost',
            'port': '5432',
            'database': 'db_sql_alchemy',
         }
        """
        return settings.DATABASES[settings.DATABASE]

    def str_connection(self):
        db = self._dict_con
        try:
            str_con = db['dialect']
            if 'driver' in db:
                str_con += '+' + db['driver']
            str_con += '://' + db['username'] + ":" + db['password'] + '@' +\
                       db['host'] + ':' + db['port'] + '/' + db['database']
        except:
            raise
        return str_con

    def is_connection_available(self):
        eng = create_engine(self.str_connection())
        with eng.connect() as con:
            return True
        return False
