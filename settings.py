import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEBUG = True

DB_TABLES = 'inout/db.json'

DATABASE = 'default'
DATABASES = {
    'default':  {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'lucasdpn',
        'password': 'tet123456',
        'host': 'localhost',
        'port': '5432',
        'database': 'db_sql_alchemy',
     },
    'postgresql':  {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'lucasdpn',
        'password': 'tet123456',
        'host': 'localhost',
        'port': '5432',
        'database': 'db_sql_alchemy',
     },
    'oracle':  {
        'dialect': 'oracle',
        'username': 'lucasdpn',
        'password': 'tet123456',
        'host': 'localhost',
        'port': '49161',
        'database': 'xe',
     }
}
