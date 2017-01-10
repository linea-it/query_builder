from utils.db import dal, str_connection

from model import operations as ops


if __name__ == "__main__":
    dal.db_init(str_connection())

    builder = ops.OperationsBuilder(
                ops.OperationsBuilder.json_to_ordered_dict('inout/data.json'))
    ops = builder.operations_list()

    for k, v in ops.items():
        print(k)
        print(v.access_data_table())
        print(str(v))
