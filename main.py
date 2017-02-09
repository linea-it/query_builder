import json
from utils.db import dal, DataAccessLayer

from model.operations_builder import OperationsBuilder
import model.tree as t

import settings


def load_json(filename):
    with open(filename) as f:
        return json.load(f)

if __name__ == "__main__":
    db = settings.DATABASES[settings.DATABASE]
    dal.db_init(DataAccessLayer.str_connection(db),
                schema_input=settings.SCHEMA_INPUT,
                schema_output=settings.SCHEMA_OUTPUT)

    obj = load_json(settings.OPERATIONS_FILE)
    tree = t.tree_builder(obj)

    print(tree)

    builder = OperationsBuilder(tree, thread_pools=10)
    operations = builder.get()
    # traverse_post_order(tree)

    for k, v in operations.items():
        print(k)
        print(str(v))
        # print(v.access_data_table())
        print(v.columns_name())
        print(v.number_of_rows())
