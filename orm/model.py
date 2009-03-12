from orm import connection
from orm.query import Expr, Select, Where, Sql


_REGISTERED = {}


class Column(Expr):
    def __init__(self, name=None, primary=False, converter=None, adapter=None):
        self.name = name
        self.primary = primary
        self.converter = converter
        self.adapter = adapter
    
    def __get__(self, obj, typ=None):
        if obj is None:
            return self
        if self.primary:
            return None
        return obj._orm_load_column(self)
    
    def sql(self, parenthesize=False):
        if hasattr(self, 'table'):
            expr = '"%s"."%s"' % (self.table, self.name)
        else:
            expr = '"%s"' % (self.name,)
        return expr, ()


class Model(object):
    class __metaclass__(type):
        def __new__(cls, name, bases, dct):
            dct['_orm_attrs'] = {}
            dct['_orm_columns'] = {}
            dct['_orm_pk_attr'] = None
            for k in dct:
                v = dct[k]
                if isinstance(v, Column):
                    v.table = dct['_orm_table']
                    if v.name is None:
                        v.name = k
                    dct['_orm_attrs'][v.name] = k
                    dct['_orm_columns'][k] = v.name
                    if v.primary:
                        dct['_orm_pk_attr'] = k
            if dct['_orm_pk_attr'] is None:
                dct['pk'] = Column(name='oid', primary=True)
                if '_orm_table' in dct:
                    dct['pk'].table = dct['_orm_table']
                dct['_orm_pk_attr'] = dct['_orm_attrs']['oid'] = 'pk'
                dct['_orm_columns']['pk'] = 'oid'
            elif 'pk' not in dct:
                dct['pk'] = dct[dct['_orm_pk_attr']]
            dct['_orm_dirty_attrs'] = set()
            inst = type.__new__(cls, name, bases, dct)
            _REGISTERED[name] = inst
            return inst
    
    _orm_new_row = True
    
    def __setattr__(self, name, value):
        if name in self._orm_columns:
            if name == self._orm_pk_attr and not self._orm_new_row:
                self._orm_old_pk = self.pk
            self._orm_dirty_attrs.add(name)
        super(Model, self).__setattr__(name, value)
    
    def _orm_load_column(self, column):
        if self._orm_new_row:
            return None
        q = ('select %s from %s '
             'where %s=?' % (column.name, self._orm_table,
                             self._orm_columns[self._orm_pk_attr]))
        args = (getattr(self, self._orm_pk_attr),)
        value = connection.cursor().execute(q, args).fetchone()[0]
        if column.converter is not None:
            value = column.converter(value)
        attr = self._orm_attrs[column.name]
        self._orm_setattr(attr, value)
        self._orm_dirty_attrs.discard(attr)
        return value
    
    def _orm_setattr(self, attr, value):
        return super(Model, self).__setattr__(attr, value)
    
    @classmethod
    def find(cls, *where):
        return Select(sources=[Sql(cls._orm_table)], where=Where(*where))
    
    def reload(self):
        for attr in self._orm_columns:
            if attr == self._orm_pk_attr:
                continue
            delattr(self, attr)
    
    def delete(self):
        if self._orm_new_row:
            return
        q = 'delete from %s where %s=?' % (self._orm_table,
                                           self._orm_columns[self._orm_pk_attr])
        args = (getattr(self, self._orm_pk_attr),)
        connection.cursor().execute(q, args)
        self._orm_new_row = True
        self._orm_dirty_attrs.update(self._orm_columns)
        self._orm_dirty_attrs.remove(self._orm_pk_attr)
        delattr(self, self._orm_pk_attr)
    
    def save(self):
        attrs = list(self._orm_dirty_attrs)
        if attrs:
            columns = []
            args = []
            for attr in attrs:
                columns.append(self._orm_columns[attr])
                adapter = getattr(type(self), attr).adapter
                value = getattr(self, attr)
                if adapter is not None:
                    value = adapter(value)
                args.append(value)
            if self._orm_new_row:
                q = ('insert into %s '
                     '(%s) values (%s)' % (self._orm_table,
                                           ', '.join(columns),
                                           ', '.join('?' * len(columns))))
            else:
                q = ('update %s '
                     'set %s where %s=?' % (self._orm_table,
                                            ', '.join(c+'=?' for c in columns),
                                            self._orm_columns[self._orm_pk_attr]))
                if self._orm_pk_attr in self._orm_dirty_attrs:
                    args.append(self._orm_old_pk)
                    del self._orm_old_pk
                else:
                    args.append(getattr(self, self._orm_pk_attr))
        elif self._orm_new_row:
            q = 'insert into %s default values' % (self._orm_table,)
            args = ()
        else:
            return
        cursor = connection.cursor()
        cursor.execute(q, args)
        if self._orm_new_row:
            self._orm_new_row = False
            self._orm_setattr(self._orm_pk_attr, cursor.lastrowid)
        self._orm_dirty_attrs.clear()
