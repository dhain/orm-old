import sqlite3


connection = None


def connect(database, timeout=None, isolation_level=None, detect_types=None):
    global connection
    if detect_types is None:
        detect_types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    kw = dict(detect_types=detect_types)
    if timeout is not None:
        kw['timeout'] = timeout
    if isolation_level is not None:
        kw['isolation_level'] = isolation_level
    connection = sqlite3.connect(database, **kw)


class printing_cursor(object):
    def __init__(self, cursor):
        self.cursor = cursor

    def __getattr__(self, name):
        if name == 'cursor':
            return super(printing_cursor, self).__getattr__(name)
        return getattr(self.cursor, name)

    def execute(self, sql, *args):
        print sql, args
        return self.cursor.execute(sql, *args)


def cursor():
    global connection
    if connection is None:
        raise RuntimeError('not connected')
    #return printing_cursor(connection.cursor())
    return connection.cursor()


def commit():
    global connection
    if connection is None:
        raise RuntimeError('not connected')
    connection.commit()
