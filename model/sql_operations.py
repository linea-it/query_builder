from sqlalchemy import Integer
from sqlalchemy.sql.expression import bindparam, ClauseElement, Executable
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import ColumnElement, Visitable, Grouping
from sqlalchemy.sql.operators import custom_op, is_precedent
from sqlalchemy.dialects import oracle


class BitwiseAnd(ColumnElement):
    type = Integer()
    operator = custom_op("&", precedence=6)

    def __init__(self, left, right):
        if not isinstance(left, Visitable):
            left = bindparam("bitand", left, unique=True)
        if not isinstance(right, Visitable):
            right = bindparam("bitand", right, unique=True)
        self.left = left
        self.right = right

    def self_group(self, against=None):
        if is_precedent(self.operator, against):
            return Grouping(self)
        else:
            return self


@compiles(BitwiseAnd)
def _compile_bitwise_and(element, compiler, **kwargs):
    left = element.left.self_group(against=element.operator)
    right = element.right.self_group(against=element.operator)
    return compiler.process(element.operator(left, right))


@compiles(BitwiseAnd, "oracle")
def _compile_bitwise_and_oracle(element, compiler, **kwargs):
    return str("BitAnd(" + str(element.left) + ", " + str(element.right) + ")")
    # return compiler.process(func.BITAND(element.left, element.right))


class CreateTableAs(Executable, ClauseElement):
    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs)
def _create_table_as(element, compiler, **kw):
    return "CREATE TABLE %s AS (%s)" % (
        element.name,
        compiler.process(element.query)
    )
