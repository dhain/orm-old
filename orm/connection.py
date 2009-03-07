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


def cursor():
    global connection
    if connection is None:
        raise RuntimeError('not connected')
    return connection.cursor()
