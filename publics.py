import sys
sys.path.append('/root/dev/app')
import requests
from datetime import datetime
db_name = 'ldb'


def PrintException():
    import linecache
    import sys
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    # return 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)


def set_db(name):
    global db_name
    db_name = name


def set_test_mode(mode):
    consts.TEST_MODE = mode


def es():
    from elasticsearch import Elasticsearch
    return Elasticsearch('localhost')


def db():
    from pymongo import MongoClient
    con = MongoClient()
    return con[db_name]


def load_messages():
    messages = {}
    try:

        set_db(db_name)
        col_server_messages = db()['server_messages']
        for item in col_server_messages.find():
            group = item['group']
            name = item['name']
            if group not in messages: messages[group] = {}
            del item['group']
            del item['name']
            messages[group][name] = item
    except:

        PrintException()
    return messages


def load_notifications():
    notifications = {}
    try:
        set_db(db_name)
        col_server_notifications = db()['server_notifications']
        for item in col_server_notifications.find():
            group = item['group']
            name = item['name']
            if group not in notifications: notifications[group] = {}
            del item['_id']
            del item['group']
            del item['name']
            notifications[group][name] = item
    except:
        PrintException()
    return notifications


class consts:
    import os
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    page_size = 20
    MAX_TOKEN_DURATION = 1000000
    MESSAGES = load_messages()
    NOTIFICATIONS = load_notifications()
    CONSOLE_LOG = True
    LOG_ACTIVE = False
    PDP_ROOT = '/var/www/html/ldb/'
    MESSAGE_SERVER_ADDRESS = 'http://logs.onmiz.org'
    MESSAGE_SERVER_PORT = '7070'
    PDP_IMAGES = PDP_ROOT + 'images/'
    SERVER_ADDRESS = 'https://server1.onmiz.org'
    SERVER_PORT = '8585'
    DB_NAME = 'ldb'
    TEST_DB_NAME = 'tdb_name'
    ODP_ROOT = SERVER_ADDRESS + '/app/'
    LOG_SERVER = 'http://logs.onmiz.org:8080'
    ODP_IMAGES = ODP_ROOT + 'images/'
    TEST_MODE = False


def create_md5(str):
    import hashlib
    ps = hashlib.md5()
    ps.update(str.encode('utf-8'))
    _hash = ps.hexdigest()
    ps = hashlib.sha1()
    ps.update(str.encode('utf-8'))
    _hash += ps.hexdigest()[:18:-1]
    _hash = _hash[::-1]
    ps = hashlib.new('ripemd160')
    ps.update(_hash.encode('utf-8'))
    return ps.hexdigest()[3:40]


def encode_token(data):
    import jwt
    import datetime
    data['date'] = str(datetime.datetime.now())
    return jwt.encode(data, 'ThisIsASecret@2019', algorithm='HS256')


def decode_token(token):
    import jwt
    try:
        result = jwt.decode(token, 'ThisIsASecret@2019', algorithms=['HS256'])
    except:
        result = None
        PrintException()
    return result


def random_str(length):
    import random, string
    return ''.join(random.choice(string.lowercase) for i in range(length))


def random_digits():
    from random import randint
    return str(randint(1000, 9999))


def log_status(l):
    from datetime import datetime
    col = db()['logs']
    del l['date']
    l['date'] = datetime.now()
    col.insert(l)


def send_notification(action, user_id, id, title, text, user={}, delayed=False, data={}, date=datetime.now()):
    try:
        if not consts.TEST_MODE:
            col_sessions = db()['sessions']
            results = col_sessions.find({'user_id': user_id})
            notif_ids = []
            for item in results:
                if item['notif_id'] is not None:
                    notif_ids.append(item['notif_id'])

            if notif_ids != []:
                param_data = {
                    'action': action,
                    'user_id': str(user_id),
                    'id': str(id),
                    'title': title,
                    'text': text,
                    'user': user,
                    'data': data,
                    'date': str(date),
                    'api_key': "AAAASYpLScM:APA91bGRykZH0bmzyTEizsopQZROcTEi902U_EyUjSySDwk-cuBmlMelsgw7SJZ76bo-jSWuM36bJBDB_7bnN4h5vpwmzzFGUwTOlQUpE1137muusHKapaxEV7CljY8K2RzCPBS9jRNdwkYS21UDy1Iwi4nC0f0tzw",
                    'notif_ids': notif_ids,
                }
                print(param_data)
                print('%s:%s/v1/notifications' % (consts.MESSAGE_SERVER_ADDRESS, consts.MESSAGE_SERVER_PORT))
                print(requests.post(url='%s:%s/v1/notifications' % (consts.MESSAGE_SERVER_ADDRESS, consts.MESSAGE_SERVER_PORT), json=param_data).json())
    except:
        PrintException()
