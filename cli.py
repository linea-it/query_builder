from utils.db import dal, DataAccessLayer
from utils import util
import networkx as nx

from model.query_builder import QueryBuilder

import settings


if __name__ == "__main__":
    db = settings.DATABASES[settings.DATABASE]
    dal.db_init(DataAccessLayer.str_connection(db),
                schema_output=settings.SCHEMA_OUTPUT)

    op_description = util.load_json(settings.OPS_DESCRIPTION_FILE)
    G=nx.read_adjlist(settings.OPS_SEQUENCE_FILE, create_using=nx.DiGraph())

    builder = QueryBuilder(op_description, G)
    operations = builder.get_operations()

    for k, v in operations.items():
        print(k)
        print(str(v))
        # print(v.access_data_table())
        print(v.columns_name())
        print(v.number_of_rows())
