# OPERATIONS_FILE = 'test/config_devel2/galaxy_properties.json'
OPERATIONS_FILE = 'test/config_y1a1_subset/footprint_inner_left.json'

SCHEMA_OUTPUT = 'tst_oracle_output'
SCHEMA_INPUT = 'tst_oracle_input'

DATABASE = 'y1a1_subset'
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
    'y1a1_subset':  {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'username': 'postgres',
        'password': 'tet123456',
        'host': 'localhost',
        'port': '5432',
        'database': 'query_builder',
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
