import sys
sys.path.append('/home/oem/dev/log_server')
sys.path.append('/root/dev/log_server')
from publics import db, create_md5, set_db
from bson import ObjectId
set_db('ldb')


def insert_users():
    col_users = db()['users']
    # col_users.drop()
    if col_users.count({'username': 'admin'}) == 0:
        col_users.insert_one({
            "_id": ObjectId("5e25ab698c90582c5785d291"),
            'name': 'shahab',
            'family': 'qazavi',
            'mobile': 'admin',
            'password': create_md5('1'),
            'role': 'admin',
        })


def insert_messages():
    col_server_messages = db()['server_messages']
    col_server_messages.drop()
    col_server_messages.insert_many([
        {
            'group': 'user',
            'name': 'token_not_received',
            'code': 401,
            'status': False,
            'en': 'Token not received'
        },
        {
            'group': 'user',
            'name': 'token_validated',
            'code': 200,
            'status': True,
            'en': 'Token validated'
        },
        {
            'group': 'user',
            'name': 'user_not_exists',
            'code': 401,
            'status': False,
            'en': 'User not exists'
        },
        {
            'group': 'user',
            'name': 'token_expired',
            'code': 401,
            'status': False,
            'en': 'User Token Expired'
        },
        {
            'group': 'user',
            'name': 'access_denied',
            'code': 401,
            'status': False,
            'en': 'Access denied'
        },
        {
            'group': 'user',
            'name': 'permission_not_defined',
            'code': 401,
            'status': False,
            'en': 'Permission not defined'
        },
        {
            'group': 'user',
            'name': 'method_not_specified',
            'code': 401,
            'status': False,
            'en': 'Method not specified'
        },
        {
            'group': 'user',
            'name': 'access_granted',
            'code': 200,
            'status': True,
            'en': 'Access granted'
        },
        {
            'group': 'public_operations',
            'name': 'params_loaded',
            'code': 200,
            'status': True,
            'en': 'Params loaded'
        },
        {
            'group': 'public_operations',
            'name': 'params_not_loaded',
            'code': 401,
            'status': False,
            'en': 'Params not loaded'
        },
        {
            'group': 'public_operations',
            'name': 'page_limit',
            'code': 401,
            'status': False,
            'en': 'Page limit reached'
        },
        {
            'group': 'public_operations',
            'name': 'record_not_found',
            'code': 401,
            'status': False,
            'en': 'Record not found'
        },
        {
            'group': 'public_operations',
            'name': 'failed',
            'code': 401,
            'status': False,
            'en': 'Operation failed'
        },
        {
            'group': 'public_operations',
            'name': 'successful',
            'code': 200,
            'status': True,
            'en': 'Operation successful'
        },
        {
            'group': 'field_error',
            'name': 'required',
            'code': 401,
            'status': False,
            'en': 'Field %s required'
        },
        {
            'group': 'field_error',
            'name': 'null',
            'code': 401,
            'status': False,
            'en': 'Null not allowed'
        },
        {
            'group': 'field_error',
            'name': 'id_format',
            'code': 401,
            'status': False,
            'en': 'ID format is not correct'
        },
        {
            'group': 'user',
            'name': 'login_failed',
            'code': 401,
            'status': False,
            'en': 'Login information is not correct'
        },
        {
            'group': 'user',
            'name': 'logged_in',
            'code': 200,
            'status': True,
            'en': 'Logged in'
        },
        {
            'group': 'user',
            'name': 'user_exist',
            'code': 200,
            'status': True,
            'en': 'User Exist'
        },
        {
            'group': 'user',
            'name': 'wrong_token',
            'code': 401,
            'status': False,
            'en': 'Wrong Token'
        }
    ])


insert_users()
insert_messages()
