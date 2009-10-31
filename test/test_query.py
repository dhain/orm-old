from nose.tools import assert_raises

from orm.query import *


def test_expr_sql():
    e = Expr(1)
    assert e.sql() == '?', e.sql()


def test_expr_sql_with_expr_value():
    class fake_expr(object):
        def sql(self):
            return 'blah'
    e = Expr(fake_expr())
    assert e.sql() == 'blah', e.sql()


def test_expr_args():
    e = Expr(1)
    assert e.args() == [1], e.args()


def test_expr_args_with_expr_value():
    class fake_expr(object):
        def args(self):
            return [1, 2, 3]
    e = Expr(fake_expr())
    assert e.args() == [1, 2, 3], e.args()


def test_binaryop():
    class concrete(BinaryOp):
        _op = 'my_op'
    e = concrete(1, 2)
    assert e.sql() == '? my_op ?', e.sql()
    assert e.args() == [1, 2], e.args()


def test_binaryop_with_exprs():
    class concrete(BinaryOp):
        _op = 'my_op'
    class fake_expr(object):
        def __init__(self, sql, args):
            self._sql = sql
            self._args = args
        def sql(self):
            return self._sql
        def args(self):
            return self._args
    e = concrete(fake_expr('left_sql', ['left_value']),
                 fake_expr('right_sql', ['right_value']))
    assert e.sql() == 'left_sql my_op right_sql', e.sql()
    assert e.args() == ['left_value', 'right_value'], e.args()


def test_expr_binary_ops():
    left = Expr(1)
    right = Expr(2)
    for op, expected, sql_op in [
        ('__lt__', Lt, '<'),
        ('__le__', Le, '<='),
        ('__eq__', Eq, '='),
        ('__gt__', Gt, '>'),
        ('__ge__', Ge, '>='),
        ('__ne__', Ne, '!='),
        ('__and__', And, 'and'),
        ('__or__', Or, 'or'),
        ('__add__', Add, '+'),
        ('__sub__', Sub, '-'),
        ('__mul__', Mul, '*'),
        ('__div__', Div, '/'),
        ('__mod__', Mod, '%'),
        ('is_in', In, 'in'),
        ('like', Like, 'like'),
        ('glob', Glob, 'glob'),
        ('match', Match, 'match'),
        ('regexp', Regexp, 'regexp'),
    ]:
        result = getattr(left, op)(right)
        assert isinstance(result, expected), result
        assert result.lvalue is left, result.lvalue
        assert result.rvalue is right, result.rvalue
        assert result.sql() == '? %s ?' % (sql_op,), (result.sql(), sql_op)
        assert result.args() == [1, 2], result.args()


def test_unaryop():
    class concrete(UnaryOp):
        _op = 'my_op'
    e = concrete(1)
    assert e.sql() == 'my_op ?', e.sql()
    assert e.args() == [1], e.args()


def test_unaryop_with_exprs():
    class concrete(UnaryOp):
        _op = 'my_op'
    class fake_expr(object):
        def __init__(self, sql, args):
            self._sql = sql
            self._args = args
        def sql(self):
            return self._sql
        def args(self):
            return self._args
    e = concrete(fake_expr('sql', ['value']))
    assert e.sql() == 'my_op sql', e.sql()
    assert e.args() == ['value'], e.args()


def test_expr_unary_ops():
    value = Expr(1)
    for op, expected, sql_op in [
        ('__invert__', Not, 'not'),
        ('__pos__', Pos, '+'),
        ('__neg__', Neg, '-'),
    ]:
        result = getattr(value, op)()
        assert isinstance(result, expected), result
        assert result.value is value, result.value
        assert result.sql() == '%s ?' % (sql_op,), (result.sql(), sql_op)
        assert result.args() == [1], result.args()


def test_isnull():
    e = Eq(Expr(1), None)
    assert e.sql() == '? isnull', e.sql()
    assert e.args() == [1], e.args()


def test_notnull():
    e = Ne(Expr(1), None)
    assert e.sql() == '? notnull', e.sql()
    assert e.args() == [1], e.args()


def test_raw_sql():
    e = Sql('this is some sql')
    assert e.sql() == 'this is some sql', e.sql()
    assert e.args() == [], e.args()


def test_asc():
    e = Asc(Sql('blah'))
    assert e.sql() == 'blah asc', e.sql()
    assert e.args() == [], e.args()


def test_desc():
    e = Desc(Sql('blah'))
    assert e.sql() == 'blah desc', e.sql()
    assert e.args() == [], e.args()


def test_exprlist():
    e = ExprList([
        Expr(1),
        Expr(2),
        3])
    assert e.sql() == '?, ?, ?', e.sql()
    assert e.args() == [1, 2, 3], e.args()


def test_exprlist_with_exprs():
    class fake_expr(object):
        def sql(self):
            return 'a'
        def args(self):
            return [1, 2]
    e = ExprList([
        fake_expr(),
        fake_expr(),
        fake_expr()])
    assert e.sql() == 'a, a, a', e.sql()
    assert e.args() == [1, 2, 1, 2, 1, 2], e.args()


def test_modellist():
    class fake_model(object):
        def __init__(self, table_name):
            self._orm_table = table_name
    e = ModelList([
        fake_model('table1'),
        fake_model('table2'),
        fake_model('table3')])
    assert e.sql() == 'table1, table2, table3', e.sql()
    assert e.args() == [], e.args()
