import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEBUG = True

DATABASE = 'postgresql'
DATABASES = {
    'default':  {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'lucasdpn',
        'password': 'naluau',
        'host': 'localhost',
        'port': '5432',
        'database': 'db_sql_alchemy',
     },
    'postgresql':  {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'lucasdpn',
        'password': 'naluau',
        'host': 'localhost',
        'port': '5432',
        'database': 'db_sql_alchemy',
     },
    'oracle':  {
        'dialect': 'oracle',
        'username': 'lucasdpn',
        'password': 'naluau',
        'host': 'localhost',
        'port': '49161',
        'database': 'xe',
     }
}


def str_connection(database):
    db = DATABASES[database]
    str_con = db['dialect']
    if 'driver' in db:
        str_con += '+' + db['driver']
    str_con += '://' + db['username'] + ":" + db['password'] + '@' +\
               db['host'] + ':' + db['port'] + '/' + db['database']
    return str_con
