class Expr(object):
    def __init__(self, *args):
        self.args = args
    
    def __lt__(self, other):
        return Expr(self, Sql('<'), other)
    
    def __le__(self, other):
        return Expr(self, Sql('<='), other)
    
    def __eq__(self, other):
        if other is None:
            return Expr(self, Sql('isnull'))
        return Expr(self, Sql('='), other)
    
    def __gt__(self, other):
        return Expr(self, Sql('>'), other)
    
    def __ge__(self, other):
        return Expr(self, Sql('>='), other)
    
    def __ne__(self, other):
        if other is None:
            return Expr(self, Sql('notnull'))
        return Expr(self, Sql('!='), other)
    
    def __and__(self, other):
        return Expr(self, Sql('and'), other)
    
    def __or__(self, other):
        return Expr(self, Sql('or'), other)
    
    def __add__(self, other):
        return Expr(self, Sql('+'), other)
    
    def __sub__(self, other):
        return Expr(self, Sql('-'), other)
    
    def __mul__(self, other):
        return Expr(self, Sql('*'), other)
    
    def __div__(self, other):
        return Expr(self, Sql('/'), other)
    
    def __mod__(self, other):
        return Expr(self, Sql('%'), other)
    
    def __invert__(self):
        return Expr(Sql('not'), self)
    
    def __pos__(self):
        return Expr(Sql('+'), self)
    
    def __neg__(self):
        return Expr(Sql('-'), self)
    
    def is_in(self, other):
        return Expr(self, Sql('in'), other)
    
    def like(self, other):
        return Expr(self, Sql('like'), other)
    
    def glob(self, other):
        return Expr(self, Sql('glob'), other)
    
    def match(self, other):
        return Expr(self, Sql('match'), other)
    
    def regexp(self, other):
        return Expr(self, Sql('regexp'), other)
    
    def sql(self, parenthesize=False):
        all_exprs = []
        all_args = []
        for part in self.args:
            if hasattr(part, 'sql'):
                expr, args = part.sql(True)
                all_exprs.append(expr)
                all_args.extend(args)
            else:
                all_exprs.append('?')
                all_args.append(part)
        expr = ' '.join(all_exprs)
        if parenthesize and len(all_exprs) > 1:
            expr = '(%s)' % (expr,)
        return expr, all_args


class Sql(Expr):
    def __init__(self, value):
        self.value = value
    
    def sql(self, parenthesize=False):
        return self.value, ()


class ExprList(Expr):
    def __init__(self, args):
        self.args = args
    
    def sql(self, parenthesize=False):
        all_exprs = []
        all_args = []
        for arg in self.args:
            if not hasattr(arg, 'sql'):
                arg = Expr(arg)
            expr, args = arg.sql(True)
            all_exprs.append(expr)
            all_args.extend(args)
        expr = ', '.join(all_exprs)
        if parenthesize and len(all_exprs) > 1:
            expr = '(%s)' % (expr,)
        return expr, all_args


class Where(object):
    def __init__(self, expr, *ands):
        if not ands:
            self.expr = expr
        else:
            self.expr = reduce(lambda e1, e2: e1 & e2, ands, expr)
    
    def sql(self, parenthesize=False):
        expr, args = self.expr.sql(True)
        return 'where %s' % (expr,), args


class Select(Expr):
    def __init__(self, exprs=(Sql('*'),), sources=None, where=None):
        self.exprs = exprs
        self.sources = sources
        self.where = where
    
    @property
    def args(self):
        args = [Sql('select'), ExprList(self.exprs)]
        if self.sources is not None:
            args.extend([Sql('from'), ExprList(self.sources)])
        if self.where is not None:
            args.append(self.where)
        return args


class Delete(Expr):
    def __init__(self, sources, where=None):
        self.sources = sources
        self.where = where
    
    @property
    def args(self):
        args = [Sql('delete from'), ExprList(self.sources)]
        if self.where is not None:
            args.append(self.where)
        return args


class Insert(Expr):
    def __init__(self, table, values=None):
        self.table = table
        self.values = values
    
    @property
    def args(self):
        args = [Sql('insert into'), Sql(self.table)]
        if self.values:
            args.extend([ExprList(self.values.keys()), Sql('values'),
                         ExprList(self.values.values())])
        else:
            args.append(Sql('default values'))
        return args


class Update(Expr):
    def __init__(self, table, values, where=None):
        self.table = table
        self.values = values
        self.where = where
    
    @property
    def args(self):
        args = [Sql('update'), Sql(self.table), Sql('set'),
                ExprList([Expr(column, Sql('='), value)
                          for column, value in self.values.iteritems()])]
        if self.where is not None:
            args.append(self.where)
        return args


