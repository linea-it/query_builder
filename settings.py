# OPERATIONS_FILE = 'test/config_devel2/galaxy_properties.json'
OPERATIONS_FILE = 'test/config_devel2/footprint_inner_left.json'

SCHEMA_OUTPUT = 'tst_oracle_output'
SCHEMA_INPUT = 'tst_oracle_input'

DATABASE = 'devel2'
DATABASES = {
    'local':  {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'lucasdpn',
        'password': 'tet123456',
        'host': 'localhost',
        'port': '5432',
        'database': 'db_sql_alchemy',
     },
    'devel2':  {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'gavo',
        'password': 'gavo',
        'host': 'localhost',
        'port': '25432',
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
