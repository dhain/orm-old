from nose.tools import assert_raises

from orm.query import *


deletes = [
    ([Sql('1')], {},
     'delete from 1', [],
     'test_delete_with_sql'),

    ([Expr(1)], {},
     'delete from ?', [1],
     'test_delete_with_expr'),

    ([Sql('1')], dict(where=Sql('a_where_clause')),
     'delete from 1 where a_where_clause', [],
     'test_delete_with_where'),

    ([Sql('1')], dict(order=Sql('an_order_clause')),
     'delete from 1 order by an_order_clause', [],
     'test_delete_with_order'),

    ([Sql('1')], dict(slice=slice(10)),
     'delete from 1 limit 10', [],
     'test_delete_with_slice'),
]


def test_delete():
    for args, kwargs, expected_sql, expected_args, name in deletes:
        def the_test():
            s = Delete(*args, **kwargs)
            assert s.sql() == expected_sql, (s.sql(), expected_sql)
            assert s.args() == expected_args, (s.args(), expected_args)
        test_delete.__name__ = 'test_delete.' + name
        yield the_test
    test_delete.__name__ = 'test_delete'


def test_delete_from_multiple_sources_raises_typeerror():
    assert_raises(TypeError, Delete, ExprList([Sql('1'), Sql('2')]))
