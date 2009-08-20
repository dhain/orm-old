from weakref import WeakValueDictionary

from orm import connection
from orm.query import *


_REGISTERED = {}


__all__ = 'Column SqlColumn ToOne ToMany ManyToMany Model'.split()


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


class SqlColumn(Column):
    def __init__(self, expr, args=(), name=None, converter=None, adapter=None):
        super(SqlColumn, self).__init__(name, False, converter, adapter)
        self.sql_expr = expr
        self.sql_args = args

    def _bind(self, model):
        return BoundSqlColumn(model, self.sql_expr, self.sql_args,
                              self.name, self.converter,
                              self.adapter)

    def sql(self):
        return self.sql_expr + ' as "%s"' % (self.name,)

    def args(self):
        return self.sql_args


class BoundColumn(Column):
    def __init__(self, model, *args, **kwargs):
        super(BoundColumn, self).__init__(*args, **kwargs)
        self.model = model


class BoundSqlColumn(SqlColumn):
    def __init__(self, model, *args, **kwargs):
        super(BoundSqlColumn, self).__init__(*args, **kwargs)
        self.model = model


class Reference(object):
    def __init__(self, my_column, other_column):
        self.my_column = my_column
        self.other_column = other_column

    def _column_by_name(self, name):
        model, attr = name.split('.')
        try:
            return getattr(_REGISTERED[model], attr)
        except KeyError:
            raise RuntimeError('by-name model reference for '
                               'unregistered model %s' % (model,))
        except AttributeError:
            raise RuntimeError('by-name column reference for '
                               'unknown column %s.%s' % (model, attr))

    def _promote_by_name(self):
        if isinstance(self.other_column, basestring):
            self.other_column = self._column_by_name(self.other_column)


class ToOne(Reference, Expr):
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
        if value is None:
            obj._orm_set_column(self.my_column, None)
        else:
            if not isinstance(value, self.other_column.model):
                raise TypeError('object must be of type %r' %
                                (self.other_column.model,))
            obj._orm_set_column(self.my_column,
                                value._orm_get_column(self.other_column))

    def __delete__(self, obj):
        self._promote_by_name()
        obj._orm_del_column(self.my_column)

    def sql(self):
        return self.my_column.sql()

    def args(self):
        return self.my_column.args()


class ToManyResult(Select):
    def __init__(self, reference, select):
        super(ToManyResult, self).__init__(select.what, select.sources,
                                           select.where, select.order,
                                           select.slice)
        self.reference = reference

    def add(self, obj):
        if not isinstance(obj, self.reference.other_column.model):
            raise TypeError('object must be of type %r' %
                            (self.reference.other_column.model,))
        dirty = obj._orm_dirty_attrs
        obj._orm_dirty_attrs = set()
        obj._orm_set_column(self.reference.other_column, self.where.rvalue)
        obj.save()
        obj._orm_dirty_attrs = dirty

    def clear(self):
        u = Update(self.reference.other_column.model,
                   {self.reference.other_column: None},
                   self.reference.other_column == self.where.rvalue)
        connection.cursor().execute(u.sql(), u.args())


class ToMany(Reference):
    def __get__(self, obj, cls):
        self._promote_by_name()
        if obj is None:
            return self
        value = obj._orm_get_column(self.my_column)
        return ToManyResult(self,
            self.other_column.model.find(self.other_column == value))


class ManyToManyResult(ToManyResult):
    def __init__(self, reference, select, filtered=False):
        super(ManyToManyResult, self).__init__(reference, select)
        self.filtered = filtered

    def add(self, obj):
        if not isinstance(obj, self.reference.other_column.model):
            raise TypeError('object must be of type %r' %
                            (self.reference.other_column.model,))
        model = self.reference.join_mine.model
        inst = model.__new__(model)
        inst._orm_set_column(self.reference.join_mine,
                             self.where.lvalue.rvalue)
        inst._orm_set_column(self.reference.join_other,
                             obj._orm_get_column(self.reference.other_column))
        inst.save()

    def find(self, where=None, *ands):
        find = super(ManyToManyResult, self).find(where, *ands)
        return ManyToManyResult(self.reference, find, True)

    def clear(self):
        model = self.reference.join_mine.model
        if self.filtered:
            what = ExprList([model.pk])
            s = Select(what, self.sources, self.where, self.order, self.slice)
            model.find(model.pk.is_in(s)).delete()
        else:
            model.find(self.reference.join_mine ==
                       self.where.lvalue.rvalue).delete()


class ManyToMany(Reference):
    def __init__(self, my_column, join_mine, join_other, other_column):
        self.my_column = my_column
        self.join_mine = join_mine
        self.join_other = join_other
        self.other_column = other_column

    def _promote_by_name(self):
        if isinstance(self.join_mine, basestring):
            self.join_mine = self._column_by_name(self.join_mine)
        if isinstance(self.join_other, basestring):
            self.join_other = self._column_by_name(self.join_other)
        if isinstance(self.other_column, basestring):
            self.other_column = self._column_by_name(self.other_column)

    def __get__(self, obj, cls):
        self._promote_by_name()
        if obj is None:
            return self
        value = obj._orm_get_column(self.my_column)
        return ManyToManyResult(self,
            Select(ExprList(self.other_column.model._orm_column_objects()),
                   ModelList([self.join_mine.model,
                              self.other_column.model]),
                   And(self.join_mine == value,
                       self.join_other == self.other_column)))


class Model(object):
    class __metaclass__(type):
        def __init__(cls, name, bases, ns):
            if bases == (object,):
                return
            cls._orm_attrs = {}
            cls._orm_columns = {}
            cls._orm_pk_attr = None
            for k in ns:
                v = ns[k]
                if isinstance(v, Column):
                    if v.name is None:
                        v.name = k
                    cls._orm_attrs[v.name.split(' ', 1)[0]] = k
                    cls._orm_columns[k] = v.name
                    if v.primary:
                        cls._orm_pk_attr = k
            if cls._orm_pk_attr is None:
                cls.pk = Column(name='rowid', primary=True)
                cls._orm_pk_attr = cls._orm_attrs['rowid'] = 'pk'
                cls._orm_columns['pk'] = 'rowid'
            cls._orm_obj_cache = WeakValueDictionary()
            _REGISTERED[name] = cls

    def __new__(cls, *args, **kwargs):
        self = super(Model, cls).__new__(cls)
        self._orm_dirty_attrs = set()
        return self

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

    @classmethod
    def _orm_column_objects(cls):
        return [getattr(cls, a) for a in cls._orm_attrs.values()]

    def _orm_where_pk(self, old=False):
        pk = self._orm_old_pk if old else self.pk
        return type(self).pk == pk

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
        for i, column in enumerate(description):
            if column[0] == cls.pk.name:
                pk = row[i]
                if cls.pk.converter is not None:
                    pk = cls.pk.converter(pk)
                break
        else:
            raise TypeError('primary key must be present in arguments')
        if pk in cls._orm_obj_cache:
            self = cls._orm_obj_cache[pk]
        else:
            self = cls.__new__(cls)
            self._orm_new_row = False
        for i, column in enumerate(description):
            column = column[0]
            value = row[i]
            try:
                attr = self._orm_attrs[column]
            except KeyError:
                self._orm_setattr(column, value)
                return
            if not attr in self._orm_dirty_attrs:
                column = getattr(cls, attr)
                if column.converter is not None:
                    value = column.converter(value)
                self._orm_setattr(attr, value)
        cls._orm_obj_cache[pk] = self
        return self

    @classmethod
    def find(cls, where=None, *ands):
        if ands:
            where = reduce(And, ands, where)
        return Select(ExprList(cls._orm_column_objects()),
                      ModelList([cls]), where)

    @classmethod
    def get(cls, pk):
        try:
            return Select(ExprList(cls._orm_column_objects()),
                          ModelList([cls]), cls.pk == pk)[0]
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
        if self.pk in self._orm_obj_cache:
            del self._orm_obj_cache[self.pk]
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
            q = Insert(self, values)
        else:
            if self._orm_pk_attr in self._orm_dirty_attrs:
                where = self._orm_where_pk(True)
                if self._orm_old_pk in self._orm_obj_cache:
                    del self._orm_obj_cache[self._orm_old_pk]
                del self._orm_old_pk
            else:
                where = self._orm_where_pk()
            q = Update(self, values, where)
        cursor = connection.cursor()
        cursor.execute(q.sql(), q.args())
        if self._orm_new_row:
            self._orm_new_row = False
            self._orm_setattr(self._orm_pk_attr, cursor.lastrowid)
        self._orm_dirty_attrs.clear()
        self._orm_obj_cache[self.pk] = self
