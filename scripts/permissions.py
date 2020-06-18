import sys
sys.path.append('/home/oem/dev/log_server')
from publics import db, set_db
from bson import ObjectId
set_db('ldb')


col_users_roles = db()['users_roles']
col_users_roles.drop()
col_users_roles.insert_many([
    {
        "_id": ObjectId("5e25ab698c90582c5785d291"),
        'name': 'admin',
        'module': 'users',
        'permissions': {
            'allow': ['get'],
        },
    },
    {
        'name': 'user',
        'module': 'profile',
        'permissions': {
            'allow': ['get', 'put'],
            'get': {'user_id':'$uid'},
            'put': {
                'query': {'user_id':'$uid'},
                'set': {}

            },
        },
    },
    {
        'name': 'admin',
        'module': 'logs',
        'permissions': {
            'allow': ['get','post'],
            'get': {},
            'post': {}
        },
    },
    {
        'name': 'admin',
        'module': 'collect',
        'permissions': {
            'allow': ['post', 'get'],
            'get': {},
            'post': {}

        },
    },
    {
        'name': 'admin',
        'module': 'customlog',
        'permissions': {
            'allow': ['get'],
            'get': {}


        },
    },
    {
        'name': 'admin',
        'module': 'lastdashboard',
        'permissions': {
            'allow': ['get'],
            'get': {}

        },
    },
    {
        'name': 'admin',
        'module': 'dashboard',
        'permissions': {
            'allow': ['get'],
            'get': {}

        },
    },

])
