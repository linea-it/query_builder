from utils.db import dal, str_connection

from model.operations import OperationsBuilder as OB
import settings


if __name__ == "__main__":
    dal.db_init(str_connection())

    builder = OB(OB.json_to_ordered_dict(settings.PATH_OPS_DESCRIPTION))
    operations = builder.get()

    for k, v in operations.items():
        print(k)
        print(v.access_data_table())
        print(str(v))
