from sqlalchemy.sql import select
from sqlalchemy import Table
from sqlalchemy.sql.expression import literal_column

from utils.db import dal


class Statement():
    def get_statement(self, params, sub_operations):
        raise NotImplementedError("Implement this method")


class GreatEqual(Statement):
    OP = "great_equal"

    def get_statement(self, params, sub_operations):
        key, value = list(params.items())[0]
        table = dal.tables[value['db']]
        stm = select(
          [table]).where(table.c.signal >= literal_column(value['value']))
        return stm


class CombinedMaps(Statement):
    OP = 'join'

    def get_statement(self, params, sub_operations):
        sub_op_names = list(params[list(params.keys())[0]]['sub_op'].keys())

        # load tables.
        sub_tables = []
        for table in sub_op_names:
            sub_tables.append(Table(sub_operations['sub_op'][table].save_at(),
                                    dal.metadata, autoload=True))

        # join statement
        stm = select([sub_tables[0]])
        stm_join = sub_tables[0]
        for i in range(1, len(sub_tables)):
            stm_join = stm_join.join(sub_tables[i], sub_tables[i-1].c.pixel ==
                                     sub_tables[i].c.pixel)
        stm = stm.select_from(stm_join)

        # REVIEW
        # drop sub tables
        # for k, v in sub_operations['sub_op'].items():
        #     v.delete()
        return stm
