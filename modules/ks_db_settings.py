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

def setting(key):
    db_name = os.environ.get('KS_DB') or 'default'
    return DB.get(db_name, {}).get(key, '')
