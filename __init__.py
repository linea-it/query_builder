from utils.db import dal, DataAccessLayer

from model.operations import OperationsBuilder as OB
import settings


if __name__ == "__main__":
    db = settings.DATABASES[settings.DATABASE]
    dal.db_init(DataAccessLayer.str_connection(db))

    builder = OB(OB.json_to_ordered_dict(settings.OPERATIONS_FILE))
    operations = builder.get()

    for k, v in operations.items():
        print(k)
        print(str(v))
