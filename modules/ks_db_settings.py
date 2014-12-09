import os
import MySQLdb

DB = {
    'dev-local':{
        'host':'127.0.0.1',
        'user':'root',
        'database':'source',
        # 'password':'source',
    },
    'circle-local':{
        'host':'127.0.0.1',
        'user':'ubuntu',
        'database':'circle_test',
        'password':'',
    },
    'circle-cloud':{
        'host':'173.194.87.126',
        'user':'root',
        'database':'source',
        'password':'ZVYZM KMGYH',
    },
    'dev-cloud':{
        'host':'173.194.87.126',
        'user':'root',
        'database':'source',
        'password':'ZVYZM KMGYH',
    },
}

def reset_all(db):
    from ks_merge import merge
    from ks_analytics import analytics
    from ks_precompute import precompute
    from ks_filehandler import filehandler

    for klass in [merge, analytics, precompute, filehandler]:
        instance = klass(db)
        instance.reset()


def get_db_name():
    from_file = None
    if os.path.isfile('ks_db_name.txt'):
        with open('ks_db_name.txt', 'r') as f:
            from_file = f.readline().strip()

    return from_file or 'gae-cloud'

def connect():
    db_name = get_db_name()

    if db_name == 'gae-cloud':
        _INSTANCE_NAME = 'ks-sqlengine:test'
        return MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME,
                db='source', user='root')
    else:
        if db_name not in DB:
            raise ValueError('Unknown db %s' % db_name)
        return MySQLdb.connect(
                setting('host'), 
                setting('user'), 
                setting('password'), 
                setting('database'))

def setting(key):
    db_name = get_db_name()
    return DB.get(db_name, {}).get(key, '')
