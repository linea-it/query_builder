from utils.db import dal, str_connection

from model import operations


if __name__ == "__main__":
    dal.db_init(str_connection())
    dal.load_tables()

    builder = operations.QueryBuilder()
    ops = builder.operations_list()

    for k, v in ops.items():
        print(k)
        print(v.access_data_table())
        print(str(v))
