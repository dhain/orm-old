from nose.tools import assert_raises

from orm.query import *


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
