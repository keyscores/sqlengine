import os

DB = {
    'local':{
        'host':'127.0.0.1',
        'user':'sqlengine_test',
        'database':'sqlengine_test',
        'password':'ZVYZM KMGYH',
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

def setting(key):
    from_file = None
    if os.path.isfile('ks_db_name.txt'):
        with open('ks_db_name.txt', 'r') as f:
            from_file = f.readline().strip()


    db_name = from_file or os.environ.get('KS_DB') or 'default'
    return DB.get(db_name, {}).get(key, '')
