from orm import connection


_REGISTERED = {}


class Column(object):
    def __init__(self, name=None, primary=False, converter=None, adapter=None):
        self.name = name
        self.primary = primary
        self.converter = converter
        self.adapter = adapter
    
    def __get__(self, obj, typ=None):
        if obj is None:
            return self
        obj._orm_load()
        return getattr(obj, obj._orm_attrs[self.name])


class ModelMeta(type):
    def __new__(cls, name, bases, dct):
        dct['_orm_attrs'] = {}
        dct['_orm_columns'] = {}
        for k in dct:
            v = dct[k]
            if isinstance(v, Column):
                if v.name is None:
                    v.name = k
                dct['_orm_attrs'][v.name] = k
                dct['_orm_columns'][k] = v.name
                if v.primary:
                    dct['_orm_pk_attr'] = k
        if '_orm_pk_attr' not in dct:
            dct['_orm_pk_attr'] = '_orm_pk'
            dct['_orm_pk'] = Column(name='oid')
            dct['_orm_attrs']['oid'] = '_orm_pk'
            dct['_orm_columns']['_orm_pk'] = 'oid'
        dct['_orm_dirty_attrs'] = set()
        inst = type.__new__(cls, name, bases, dct)
        _REGISTERED[name] = inst
        return inst


class Model(object):
    __metaclass__ = ModelMeta
    
    _orm_new_row = True
    
    def __setattr__(self, name, value):
        if name in self._orm_columns:
            if name == self._orm_pk_attr and not self._orm_new_row:
                self._orm_old_pk = self.pk
            self._orm_dirty_attrs.add(name)
        super(Model, self).__setattr__(name, value)
    
    def pk():
        def fset(self, value):
            setattr(self, self._orm_pk_attr, value)
        
        def fget(self):
            return getattr(self, self._orm_pk_attr)
        
        return locals()
    pk = property(**pk())
    
    def _orm_load(self):
        attrs = list(self._orm_columns)
        if self._orm_new_row:
            for attr in attrs:
                setattr(self, attr, None)
            return
        columns = [self._orm_columns[attr] for attr in attrs]
        q = ('select %s from %s '
             'where %s=?' % (', '.join(columns),
                             self._orm_table,
                             self._orm_columns[self._orm_pk_attr]))
        args = (self.pk,)
        row = connection.cursor().execute(q, args).fetchone()
        if row is None:
            self._orm_new_row = True
            return self._orm_load()
        for attr, value in zip(attrs, row):
            converter = getattr(type(self), attr).converter
            if converter is not None:
                value = converter(value)
            super(Model, self).__setattr__(attr, value)
        self._orm_dirty_attrs.clear()
    
    def delete(self):
        if self._orm_new_row:
            return
        q = 'delete from %s where %s=?' % (self._orm_table,
                                           self._orm_columns[self._orm_pk_attr])
        args = (self.pk,)
        connection.cursor().execute(q, args)
        self._orm_new_row = True
        self._orm_dirty_attrs.update(self._orm_columns)
        setattr(self, self._orm_pk_attr, None)
    
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
                    args.append(self.pk)
        elif self._orm_new_row:
            q = 'insert into %s default values' % (self._orm_table,)
            args = ()
        else:
            return
        cursor = connection.cursor()
        cursor.execute(q, args)
        if self._orm_new_row:
            self._orm_new_row = False
            super(Model, self).__setattr__(self._orm_pk_attr,
                                           cursor.lastrowid)
        self._orm_dirty_attrs.clear()
