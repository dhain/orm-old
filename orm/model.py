from orm import connection
from orm.query import *


_REGISTERED = {}


__all__ = 'Column ToOne ToMany Model'.split()


class Column(Expr):
    def __init__(self, name=None, primary=False, converter=None, adapter=None):
        self.name = name
        self.primary = primary
        if converter is not None:
            self.converter = converter
        if adapter is not None:
            self.adapter = adapter
    
    converter = None
    adapter = None
    
    def __get__(self, obj, cls):
        if obj is None:
            return self
        if self.primary:
            return None
        return obj._orm_load_column(self)
    
    def sql(self):
        if hasattr(self, 'table'):
            expr = '"%s"."%s"' % (self.table, self.name)
        else:
            expr = '"%s"' % (self.name,)
        return expr
    
    def args(self):
        return []


class ToOne(Column):
    def __init__(self, model, column='pk', name=None):
        self.model = model
        self.column = column
        self.name = name
        self.primary = False
    
    def _promote_by_name(self):
        if isinstance(self.model, basestring):
            try:
                self.model = _REGISTERED[self.model]
            except KeyError:
                raise RuntimeError('by-name model reference for '
                                   'unregistered model %s' % (self.model,))
        if isinstance(self.column, basestring):
            try:
                self.column = getattr(self.model, self.column)
            except AttributeError:
                raise RuntimeError('by-name column reference for '
                                   'unknown column %s' % (self.column,))
    
    def converter(self, value):
        self._promote_by_name()
        try:
            return self.model.find(self.column == value)[0]
        except IndexError:
            return None
    
    def adapter(self, value):
        self._promote_by_name()
        return getattr(value, value._orm_attrs[self.column.name])


class ToMany(object):
    def __init__(self, model, column):
        self.model = model
        self.column = column
    
    def _promote_by_name(self):
        if isinstance(self.model, basestring):
            try:
                self.model = _REGISTERED[self.model]
            except KeyError:
                raise RuntimeError('by-name model reference for '
                                   'unregistered model %s' % (self.model,))
        if isinstance(self.column, basestring):
            try:
                self.column = getattr(self.model, self.column)
            except AttributeError:
                raise RuntimeError('by-name column reference for '
                                   'unknown column %s' % (self.column,))
        self.column._promote_by_name()
    
    def __get__(self, obj, cls):
        self._promote_by_name()
        if obj is None:
            return self
        value = getattr(obj, obj._orm_attrs[self.column.column.name])
        return self.model.find(self.column == value)


class Model(object):
    class __metaclass__(type):
        def __new__(cls, name, bases, dct):
            if bases == (object,):
                return type.__new__(cls, name, bases, dct)
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
                dct['pk'].table = dct['_orm_table']
                dct['_orm_pk_attr'] = dct['_orm_attrs']['oid'] = 'pk'
                dct['_orm_columns']['pk'] = 'oid'
            dct['_orm_dirty_attrs'] = set()
            inst = type.__new__(cls, name, bases, dct)
            _REGISTERED[name] = inst
            return inst
    
    class pk(object):
        def __get__(self, obj, cls):
            if obj is None:
                obj = cls
            return getattr(obj, obj._orm_pk_attr)
    pk = pk()
    
    _orm_new_row = True
    
    def __setattr__(self, name, value):
        if name in self._orm_columns:
            if name == self._orm_pk_attr and not self._orm_new_row:
                self._orm_old_pk = self.pk
            self._orm_dirty_attrs.add(name)
        super(Model, self).__setattr__(name, value)
    
    def _orm_setattr(self, attr, value):
        return super(Model, self).__setattr__(attr, value)
    
    def _orm_where_pk(self, old=False):
        pk = self._orm_old_pk if old else getattr(self, self._orm_pk_attr)
        return getattr(type(self), self._orm_pk_attr) == pk
    
    def _orm_adapt_attr(self, attr):
        adapter = getattr(type(self), attr).adapter
        value = getattr(self, attr)
        if adapter is not None:
            value = adapter(value)
        return value
    
    def _orm_load_column(self, column):
        if self._orm_new_row:
            return None
        q = Select(column, Sql(self._orm_table), self._orm_where_pk())
        value = connection.cursor().execute(q.sql(), q.args()).fetchone()[0]
        if column.converter is not None:
            value = column.converter(value)
        attr = self._orm_attrs[column.name]
        self._orm_setattr(attr, value)
        self._orm_dirty_attrs.discard(attr)
        return value
    
    @classmethod
    def _orm_load(cls, row, description):
        self = cls.__new__(cls)
        for i, column in enumerate(description):
            column = column[0]
            value = row[i]
            try:
                attr = self._orm_attrs[column]
            except KeyError:
                self._orm_setattr(column, value)
                return
            column = getattr(cls, attr)
            if column.converter is not None:
                value = column.converter(value)
            self._orm_setattr(attr, value)
        return self
    
    @classmethod
    def find(cls, where=None, *ands):
        if ands:
            where = reduce(And, ands, where)
        return Select(sources=ModelList([cls]), where=where)
    
    @classmethod
    def get(cls, pk):
        try:
            return Select(sources=ModelList([cls]), where=(cls.pk == pk))[0]
        except IndexError:
            raise KeyError(pk, 'no such row')
    
    def reload(self):
        for attr in self._orm_columns:
            if attr == self._orm_pk_attr:
                continue
            delattr(self, attr)
    
    def delete(self):
        if self._orm_new_row:
            return
        q = Delete(Sql(self._orm_table), self._orm_where_pk())
        connection.cursor().execute(q.sql(), q.args())
        self._orm_new_row = True
        self._orm_dirty_attrs.update(self._orm_columns)
        self._orm_dirty_attrs.remove(self._orm_pk_attr)
        delattr(self, self._orm_pk_attr)
    
    def save(self):
        if not self._orm_dirty_attrs and not self._orm_new_row:
            return
        values = dict((getattr(type(self), attr), self._orm_adapt_attr(attr))
                      for attr in self._orm_dirty_attrs)
        if self._orm_new_row:
            q = Insert(self._orm_table, values)
        else:
            if self._orm_pk_attr in self._orm_dirty_attrs:
                where = self._orm_where_pk(True)
                del self._orm_old_pk
            else:
                where = self._orm_where_pk()
            q = Update(self._orm_table, values, where)
        cursor = connection.cursor()
        cursor.execute(q.sql(), q.args())
        if self._orm_new_row:
            self._orm_new_row = False
            self._orm_setattr(self._orm_pk_attr, cursor.lastrowid)
        self._orm_dirty_attrs.clear()
