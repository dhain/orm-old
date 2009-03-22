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
        if not hasattr(self, 'model'):
            self = self._bind(cls)
        if obj is None:
            return self
        if self.primary:
            return None
        return obj._orm_load_column(self)
    
    def _bind(self, model):
        return BoundColumn(model, self.name, self.primary,
                           self.converter, self.adapter)
    
    def sql(self):
        if hasattr(self, 'model'):
            expr = '"%s"."%s"' % (self.model._orm_table, self.name)
        else:
            expr = '"%s"' % (self.name,)
        return expr
    
    def args(self):
        return []


class BoundColumn(Column):
    def __init__(self, model, *args, **kwargs):
        super(BoundColumn, self).__init__(*args, **kwargs)
        self.model = model


class Reference(object):
    def __init__(self, my_column, other_column):
        self.my_column = my_column
        self.other_column = other_column
    
    def _promote_by_name(self):
        if isinstance(self.other_column, basestring):
            model, attr = self.other_column.split('.')
            try:
                self.other_column = getattr(_REGISTERED[model], attr)
            except KeyError:
                raise RuntimeError('by-name model reference for '
                                   'unregistered model %s' % (model,))
            except AttributeError:
                raise RuntimeError('by-name column reference for '
                                   'unknown column %s.%s' % (model, attr))


class ToOne(Reference):
    def __get__(self, obj, cls):
        self._promote_by_name()
        if obj is None:
            return self
        value = obj._orm_get_column(self.my_column)
        try:
            return self.other_column.model.find(self.other_column == value)[0]
        except IndexError:
            return None
    
    def __set__(self, obj, value):
        self._promote_by_name()
        obj._orm_set_column(self.my_column,
                            value._orm_get_column(self.other_column))
    
    def __delete__(self, obj):
        self._promote_by_name()
        obj._orm_del_column(self.my_column)


class ToMany(Reference):
    def __get__(self, obj, cls):
        self._promote_by_name()
        if obj is None:
            return self
        value = obj._orm_get_column(self.my_column)
        return self.other_column.model.find(self.other_column == value)


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
                    if v.name is None:
                        v.name = k
                    dct['_orm_attrs'][v.name] = k
                    dct['_orm_columns'][k] = v.name
                    if v.primary:
                        dct['_orm_pk_attr'] = k
            if dct['_orm_pk_attr'] is None:
                dct['pk'] = Column(name='oid', primary=True)
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
    
    def _orm_get_column(self, column):
        return getattr(self, self._orm_attrs[column.name])
    
    def _orm_set_column(self, column, value):
        return setattr(self, self._orm_attrs[column.name], value)
    
    def _orm_del_column(self, column):
        return delattr(self, self._orm_attrs[column.name])
    
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
