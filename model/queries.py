from sqlalchemy.sql import select
from sqlalchemy import (create_engine, Table, MetaData)
from sqlalchemy.sql.expression import literal_column

# from inout import input_settings
from model import sql_operations as op
from utils.db import dal


class Operation():
    def __init__(self, params, sub_operations):
        self._params = params
        self._sub_operations = sub_operations

        # define operation
        self._stm = None
        key, value = list(params.items())[0]
        self._set_operation(value['op'])

        # create temp table to let the data accessible.
        self.create()
        self.data_table = self._access_data_table()

    def _set_operation(self, operation_type):
        if operation_type == GreatEqual.OP:
            op = GreatEqual()
        elif operation_type == CombinedMaps.OP:
            op = CombinedMaps()
        else:
            raise "This operations is not registered"

        self._stm = op.get_statement(self._params, self._sub_operations)

    def __str__(self):
        return (str(self._stm))

    def operation_name(self):
        return list(self._params.keys())[0]

    def save_at(self):
        return self.operation_name() + "_" + "table"

    def create(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.CreateTableAs(self.save_at(),
                        self._stm))

    def _access_data_table(self):
        with dal.engine.connect() as con:
            table = Table(self.save_at(), dal.metadata, autoload=True)
            stmt = select([table])
            result = con.execute(stmt).fetchall()
        return result

    def access_data_table(self):
        return self.data_table

    def delete(self):
        with dal.engine.connect() as con:
            con.execute("commit")
            con.execute(op.DropTable(self.save_at()))


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
