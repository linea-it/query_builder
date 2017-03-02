from utils.db import dal, DataAccessLayer
from utils import util

from model.query_builder import QueryBuilder
import model.tree as t

import settings


if __name__ == "__main__":
    db = settings.DATABASES[settings.DATABASE]
    dal.db_init(DataAccessLayer.str_connection(db),
                schema_output=settings.SCHEMA_OUTPUT)

    ops_desc = util.load_json(settings.OPS_DESCRIPTION_FILE)
    tree_desc = util.dot_file_to_dict(settings.OPS_SEQUENCE_FILE)
    tree = t.Tree(tree_desc, ops_desc).tree_builder('galaxy_properties')

    print(tree)

    builder = QueryBuilder(tree, thread_pools=10)
    operations = builder.get()
    # traverse_post_order(tree)

    for k, v in operations.items():
        print(k)
        print(str(v))
        # print(v.access_data_table())
        print(v.columns_name())
        print(v.number_of_rows())
