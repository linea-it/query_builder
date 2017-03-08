# OPERATIONS_FILE = 'test/config_devel2/galaxy_properties.json'
OPS_DESCRIPTION_FILE = 'test/ops_y1a1.json'
OPS_SEQUENCE_FILE = 'test/operations_sequence.dot'

SCHEMA_OUTPUT = 'tst_oracle_output'

DATABASE = 'y1a1'
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
    'y1a1':  {
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
    },
    'dessci':  {
        'dialect': 'oracle',
        'username': 'brportal',
        'password': 'brp70chips',
        'host': 'leovip148.ncsa.uiuc.edu',
        'port': '1521',
        'database': 'dessci',
     }
}
