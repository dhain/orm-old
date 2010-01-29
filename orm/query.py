from orm import connection
from orm.util import slice2limit


__all__ = (
    'Expr UnaryOp BinaryOp '
    'Not Pos Neg Lt Le Eq Gt Ge Ne And Or Add Sub Mul Div Mod '
    'In Like Glob Match Regexp Sql ExprList ModelList '
    'Asc Desc Select Delete Insert Update'
).split()


class Expr(object):
    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return Lt(self, other)

    def __le__(self, other):
        return Le(self, other)

    def __eq__(self, other):
        return Eq(self, other)

    def __gt__(self, other):
        return Gt(self, other)

    def __ge__(self, other):
        return Ge(self, other)

    def __ne__(self, other):
        return Ne(self, other)

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __add__(self, other):
        return Add(self, other)

    def __sub__(self, other):
        return Sub(self, other)

    def __mul__(self, other):
        return Mul(self, other)

    def __div__(self, other):
        return Div(self, other)

    def __mod__(self, other):
        return Mod(self, other)

    def __invert__(self):
        return Not(self)

    def __pos__(self):
        return Pos(self)

    def __neg__(self):
        return Neg(self)

    def is_in(self, other):
        return In(self, other)

    def like(self, other):
        return Like(self, other)

    def glob(self, other):
        return Glob(self, other)

    def match(self, other):
        return Match(self, other)

    def regexp(self, other):
        return Regexp(self, other)

    def sql(self):
        if hasattr(self.value, 'sql'):
            return self.value.sql()
        return '?'

    def args(self):
        if hasattr(self.value, 'args'):
            return self.value.args()
        return [self.value]


class UnaryOp(Expr):
    def sql(self):
        return ' '.join((
            self._op,
            self.value.sql() if hasattr(self.value, 'sql') else '?'))


unary_ops = [
    ('Not', 'not'),
    ('Pos', '+'),
    ('Neg', '-'),
]
for classname, op in unary_ops:
    locals()[classname] = type(classname, (UnaryOp,), dict(_op=op))
del classname, op, unary_ops


class BinaryOp(Expr):
    def __init__(self, lvalue, rvalue):
        self.lvalue = lvalue
        self.rvalue = rvalue

    def sql(self):
        return ' '.join((
            self.lvalue.sql() if hasattr(self.lvalue, 'sql') else '?',
            self._op,
            self.rvalue.sql() if hasattr(self.rvalue, 'sql') else '?'))

    def args(self):
        return (
            (self.lvalue.args() if hasattr(self.lvalue, 'args') else [self.lvalue]) +
            (self.rvalue.args() if hasattr(self.rvalue, 'args') else [self.rvalue]))


binary_ops = [
    ('Lt', '<'),
    ('Gt', '>'),
    ('Le', '<='),
    ('Ge', '>='),
    ('And', 'and'),
    ('Or', 'or'),
    ('Add', '+'),
    ('Sub', '-'),
    ('Mul', '*'),
    ('Div', '/'),
    ('Mod', '%'),
    ('Like', 'like'),
    ('Glob', 'glob'),
    ('Match', 'match'),
    ('Regexp', 'regexp'),
]
for classname, op in binary_ops:
    locals()[classname] = type(classname, (BinaryOp,), dict(_op=op))
del classname, op, binary_ops


class Eq(BinaryOp):
    _op = '='

    def sql(self):
        if self.rvalue is None:
            return ' '.join((self.lvalue.sql(), 'isnull'))
        return super(Eq, self).sql()

    def args(self):
        if self.rvalue is None:
            return self.lvalue.args()
        return super(Eq, self).args()


class Ne(BinaryOp):
    _op = '!='

    def sql(self):
        if self.rvalue is None:
            return ' '.join((self.lvalue.sql(), 'notnull'))
        return super(Ne, self).sql()

    def args(self):
        if self.rvalue is None:
            return self.lvalue.args()
        return super(Ne, self).args()


class In(BinaryOp):
    _op = 'in'

    def sql(self):
        if isinstance(self.rvalue, Select):
            return '%s in (%s)' % (self.lvalue.sql(), self.rvalue.sql())
        return super(In, self).sql()


class Sql(Expr):
    def sql(self):
        return self.value

    def args(self):
        return []


class ExprList(list, Expr):
    def sql(self):
        return ', '.join((item.sql() if hasattr(item, 'sql') else '?')
                         for item in self)

    def args(self):
        args = []
        for item in self:
            if hasattr(item, 'args'):
                args.extend(item.args())
            else:
                args.append(item)
        return args


class ModelList(ExprList):
    def sql(self):
        return ', '.join((item._orm_table for item in self))

    def args(self):
        return []


class Asc(Expr):
    def sql(self):
        return super(Asc, self).sql() + ' asc'


class Desc(Expr):
    def sql(self):
        return super(Desc, self).sql() + ' desc'


class Select(Expr):
    def __init__(self, what=None, sources=None,
                 where=None, order=None, slice=None):
        if what is None:
            if sources is None:
                raise TypeError('must specify sources when not specifying what')
            what = Sql('*')
        self.what = what
        self.sources = sources
        self.where = where
        self.order = order
        self.slice = slice

    def __getitem__(self, key):
        s = Select(self.what, self.sources, self.where, self.order)
        if isinstance(key, (int, long)):
            s.slice = slice(key, key + 1)
            try:
                return iter(s).next()
            except StopIteration:
                raise IndexError(key)
        s.slice = key
        return s

    def __iter__(self):
        cursor = connection.cursor()
        result = cursor.execute(self.sql(), self.args())
        if isinstance(self.sources, ModelList):
            description = cursor.description
            for row in result:
                res = []
                for model in self.sources:
                    mdesc = tuple((description[i][0], i)
                                  for i, c in enumerate(self.what)
                                  if c.model is model)
                    if not mdesc:
                        continue
                    mrow = tuple(row[d[1]] for d in mdesc)
                    res.append(model._orm_load(mrow, mdesc))
                yield tuple(res) if len(res) > 1 else res[0]
        else:
            for row in result:
                yield row

    def __len__(self):
        s = Select(Sql('count(*)'), self.sources,
                   self.where, self.order, self.slice)
        return connection.cursor().execute(s.sql(), s.args()).fetchone()[0]

    def exists(self):
        s = Select(Sql('1'), self.sources, self.where, self.order, self.slice)
        return connection.cursor().execute(s.sql(), s.args()).fetchone() is not None

    def find(self, where=None, *ands):
        if ands:
            where = reduce(And, ands, where)
        if self.where is not None:
            where = self.where & where
        return Select(self.what, self.sources, where, self.order, self.slice)

    def order_by(self, *args):
        if self.order is not None:
            order = ExprList(self.order)
            order.extend(args)
        elif args:
            order = ExprList(args)
        else:
            order = None
        return Select(self.what, self.sources, self.where, order, self.slice)

    def delete(self):
        if self.sources is None:
            raise TypeError("can't delete without sources")
        d = Delete(self.sources, self.where, self.order, self.slice)
        connection.cursor().execute(d.sql(), d.args())

    def sql(self):
        sql = 'select ' + self.what.sql()
        if self.sources is not None:
            sql += ' from ' + self.sources.sql()
        if self.where is not None:
            sql += ' where ' + self.where.sql()
        if self.order is not None:
            sql += ' order by ' + self.order.sql()
        if self.slice is not None:
            slc = slice2limit(self.slice)
            if slc:
                sql += ' ' + slc
        return sql

    def args(self):
        args = self.what.args()
        if self.sources is not None:
            args.extend(self.sources.args())
        if self.where is not None:
            args.extend(self.where.args())
        if self.order is not None:
            args.extend(self.order.args())
        return args


class Delete(Expr):
    def __init__(self, sources, where=None, order=None, slice=None):
        if isinstance(sources, ExprList) and len(sources) > 1:
            raise TypeError("can't delete from more than one table")
        self.sources = sources
        self.where = where
        self.order = order
        self.slice = slice

    def sql(self):
        sql = 'delete from ' + self.sources.sql()
        if self.where is not None:
            sql += ' where ' + self.where.sql()
        if self.order is not None:
            sql += ' order by ' + self.order.sql()
        if self.slice is not None:
            slc = slice2limit(self.slice)
            if slc:
                sql += ' ' + slc
        return sql

    def args(self):
        args = self.sources.args()
        if self.where is not None:
            args.extend(self.where.args())
        if self.order is not None:
            args.extend(self.order.args())
        return args


class Insert(Expr):
    def __init__(self, model, values=None):
        self.model = model
        self.values = values

    def sql(self):
        sql = 'insert into ' + self.model._orm_table
        if self.values:
            sql += ' (%s) values (%s)' % (
                    ExprList(Sql(column.name) for column in self.values).sql(),
                    ExprList(self.values.values()).sql())
        else:
            sql += ' default values'
        return sql

    def args(self):
        args = []
        if self.values:
            for value in self.values.values():
                if hasattr(value, 'args'):
                    args.extend(value.args())
                else:
                    args.append(value)
        return args


class Update(Expr):
    def __init__(self, model, values, where=None):
        self.model = model
        self.values = values
        self.where = where

    def sql(self):
        values = [column.name +
                  (' = ' + value.sql() if hasattr(value, 'sql') else ' = ?')
                  for column, value in self.values.iteritems()]
        sql = 'update ' + self.model._orm_table + ' set ' + ', '.join(values)
        if self.where is not None:
            sql += ' where ' + self.where.sql()
        return sql

    def args(self):
        args = []
        for value in self.values.values():
            if hasattr(value, 'args'):
                args.extend(value.args())
            else:
                args.append(value)
        if self.where is not None:
            args.extend(self.where.args())
        return args
