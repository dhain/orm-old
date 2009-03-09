class Sql(object):
    def __init__(self, value):
        self.value = value
    
    def sql(self):
        return self.value, ()

class Expr(object):
    def __init__(self, *args):
        self.args = args
    
    def __lt__(self, other):
        return Expr(self, Sql('<'), other)
    
    def __le__(self, other):
        return Expr(self, Sql('<='), other)
    
    def __eq__(self, other):
        if other is None:
            return Expr(self, Sql(' isnull'))
        return Expr(self, Sql('='), other)
    
    def __gt__(self, other):
        return Expr(self, Sql('>'), other)
    
    def __ge__(self, other):
        return Expr(self, Sql('>='), other)
    
    def __ne__(self, other):
        if other is None:
            return Expr(self, Sql(' notnull'))
        return Expr(self, Sql('!='), other)
    
    def __and__(self, other):
        return Expr(self, Sql(' and '), other)
    
    def __or__(self, other):
        return Expr(self, Sql(' or '), other)
    
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
        return Expr(Sql('not '), self)
    
    def __pos__(self):
        return Expr(Sql('+'), self)
    
    def __neg__(self):
        return Expr(Sql('-'), self)
    
    def __contains__(self, other):
        return Expr(other, Sql(' in '), self)
    
    def like(self, other):
        return Expr(self, Sql(' like '), other)
    
    def glob(self, other):
        return Expr(self, Sql(' glob '), other)
    
    def match(self, other):
        return Expr(self, Sql(' match '), other)
    
    def regexp(self, other):
        return Expr(self, Sql(' regexp '), other)
    
    def sql(self):
        expr = ''
        args = []
        for part in self.args:
            if hasattr(part, 'sql'):
                part_expr, part_args = part.sql()
                expr += part_expr
                args.extend(part_args)
            else:
                expr += '?'
                args.append(part)
        if len(self.args) > 1:
            expr = '(%s)' % (expr,)
        return expr, args
    
    def __repr__(self):
        return '%s(%s)' % (type(self).__name__,
                           ', '.join(repr(a) for a in self.args))


class Where(object):
    def __init__(self, expr, *ands):
        if not ands:
            self.expr = expr
        else:
            self.expr = reduce(lambda e1, e2: e1 & e2, ands, expr)
    
    def sql(self):
        expr, args = self.expr.sql()
        return 'where %s' % (expr,), args
