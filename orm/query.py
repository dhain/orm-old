from orm import connection
from orm.util import slice2limit


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
    ('In', 'in'),
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
    _op = '='
    
    def sql(self):
        if self.rvalue is None:
            return ' '.join((self.lvalue.sql(), 'notnull'))
        return super(Eq, self).sql()
    
    def args(self):
        if self.rvalue is None:
            return self.lvalue.args()
        return super(Eq, self).args()


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


class ModelList(list, Expr):
    def sql(self):
        return ', '.join((item._orm_table for item in self))
    
    def args(self):
        return []


class Select(Expr):
    def __init__(self, what=None, sources=None, where=None, slice=None):
        if what is None:
            what = ExprList([Sql('*')])
        self.what = what
        self.sources = sources
        self.where = where
        self.slice = slice
    
    def __getitem__(self, key):
        s = Select(self.what, self.sources, self.where)
        if isinstance(key, int):
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
                    mdesc = tuple((d[0], i) for i, d in enumerate(description)
                                  if d[0] in model._orm_attrs)
                    mrow = tuple(row[d[1]] for d in mdesc)
                    res.append(model._orm_load(mrow, mdesc))
                yield tuple(res) if len(res) > 1 else res[0]
        else:
            for row in result:
                yield row
    
    def sql(self):
        sql = 'select ' + self.what.sql()
        if self.sources is not None:
            sql += ' from ' + self.sources.sql()
        if self.where is not None:
            sql += ' where ' + self.where.sql()
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
        return args


class Delete(Expr):
    def __init__(self, sources, where=None):
        self.sources = sources
        self.where = where
    
    def sql(self):
        sql = 'delete from ' + self.sources.sql()
        if self.where is not None:
            sql += ' where ' + self.where.sql()
        return sql
    
    def args(self):
        args = self.sources.args()
        if self.where is not None:
            args.extend(self.where.args())
        return args


class Insert(Expr):
    def __init__(self, table, values=None):
        self.table = table
        self.values = values
    
    def sql(self):
        sql = 'insert into ' + self.table
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
    def __init__(self, table, values, where=None):
        self.table = table
        self.values = values
        self.where = where
    
    def sql(self):
        values = [column.name +
                  (' = ' + value.sql() if hasattr(value, 'sql') else ' = ?')
                  for column, value in self.values.iteritems()]
        sql = 'update ' + self.table + ' set ' + ', '.join(values)
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


