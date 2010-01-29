from nose.tools import assert_raises

from orm import connection
from orm.query import *


class FakeConnection(object):
    def __init__(self, rows=((1,),)):
        self.cursors = []
        self.rows = rows

    def cursor(self):
        cursor = self._cursor(self)
        self.cursors.append(cursor)
        return cursor

    class _cursor(object):
        def __init__(self, connection):
            self.connection = connection
            self.executions = []

        def __iter__(self):
            return iter(self.connection.rows)

        def fetchone(self):
            return iter(self).next()

        def execute(self, sql, args=()):
            self.executions.append((sql, args))
            return self


def test_empty_select_raises_TypeError():
    assert_raises(TypeError, Select)


selects = [
    ([Sql('1')], {},
     'select 1', [],
     'test_select_with_sql'),

    ([Expr(1)], {},
     'select ?', [1],
     'test_select_with_expr'),

    ([], dict(sources=Sql('a_table')),
     'select * from a_table', [],
     'test_select_with_source'),

    ([Sql('1')], dict(where=Sql('a_where_clause')),
     'select 1 where a_where_clause', [],
     'test_select_with_where'),

    ([Sql('1')], dict(order=Sql('an_order_clause')),
     'select 1 order by an_order_clause', [],
     'test_select_with_order'),

    ([Sql('1')], dict(slice=slice(10)),
     'select 1 limit 10', [],
     'test_select_with_slice'),
]


def test_select():
    for args, kwargs, expected_sql, expected_args, name in selects:
        def the_test():
            s = Select(*args, **kwargs)
            assert s.sql() == expected_sql, (s.sql(), expected_sql)
            assert s.args() == expected_args, (s.args(), expected_args)
        test_select.__name__ = 'test_select.' + name
        yield the_test
    test_select.__name__ = 'test_select'


def test_iter():
    connection.connection = FakeConnection()
    it = iter(Select(Sql('1')))
    result = it.next()
    assert result == (1,), result
    execution = connection.connection.cursors[0].executions[0]
    assert execution == ('select 1', []), execution


def test_len():
    connection.connection = FakeConnection()
    result = len(Select(Sql('1')))
    assert result == 1, result
    execution = connection.connection.cursors[0].executions[0]
    assert execution == ('select count(*)', []), execution


def test_exists_returns_false():
    connection.connection = FakeConnection((None,))
    result = Select(Sql('1')).exists()
    assert not result
    execution = connection.connection.cursors[0].executions[0]
    assert execution == ('select 1', []), execution


def test_exists_returns_true():
    connection.connection = FakeConnection(((1,),))
    result = Select(Sql('1')).exists()
    assert result
    execution = connection.connection.cursors[0].executions[0]
    assert execution == ('select 1', []), execution


def test_find():
    find = Select(Sql('1')).find(Sql('2'))
    assert isinstance(find, Select), find
    assert find.sql() == 'select 1 where 2', find.sql()
    assert find.args() == [], find.args()


def test_empty_find():
    find = Select(Sql('1')).find()
    assert isinstance(find, Select), find
    assert find.where is None, find.where
    assert find.sql() == 'select 1', find.sql()
    assert find.args() == [], find.args()
