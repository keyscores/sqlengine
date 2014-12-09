import os
import MySQLdb

DB = {
    'local':{
        'host':'127.0.0.1',
        'user':'root',
        'database':'source',
        # 'password':'source',
    },
    'circleci':{
        'host':'127.0.0.1',
        'user':'ubuntu',
        'database':'circle_test',
        'password':'',
    },
    'default':{
        'host':'173.194.87.126',
        'user':'root',
        'database':'source',
        'password':'ZVYZM KMGYH',
    }
}

def reset_all(db):
    from ks_merge import merge
    from ks_analytics import analytics
    from ks_precompute import precompute
    from ks_filehandler import filehandler

    for klass in [merge, analytics, precompute, filehandler]:
        instance = klass(db)
        instance.reset()

def connect():
    from_file = None
    if os.path.isfile('ks_db_name.txt'):
        with open('ks_db_name.txt', 'r') as f:
            from_file = f.readline().strip()

    db_name = from_file or os.environ.get('KS_DB') or 'default'

    if db_name == 'cloud':
        _INSTANCE_NAME = 'ks-sqlengine:test'
        return MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME,
                db='source', user='root')
    else:

        return MySQLdb.connect(
                setting('host'), 
                setting('user'), 
                setting('password'), 
                setting('database'))

def setting(key):
    from_file = None
    if os.path.isfile('ks_db_name.txt'):
        with open('ks_db_name.txt', 'r') as f:
            from_file = f.readline().strip()


    db_name = from_file or os.environ.get('KS_DB') or 'default'
    return DB.get(db_name, {}).get(key, '')
