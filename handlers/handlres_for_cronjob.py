from bson.json_util import loads
from base_handler import BaseHandler
from publics import PrintException, create_md5, encode_token, db
from datetime import datetime , timedelta
from bson import ObjectId
import math

col_daily = db()['daily']
col_weekly = db()['weekly']
col_monthly = db()['monthly']

time_now = datetime.now()
col_dashboard_report = db()['dashboard']
col_users = db()['users']
col_services = db()['services']
col_logs = db()['logs']

first_1398 = datetime.strptime('2019-03-21', "%Y-%m-%d")
all_user = 38526
"""
Create list of user id that have services
"""

service_users_ids = []
for item in col_services.distinct('user.id'):
    if item is not None:
        service_users_ids.append(ObjectId(item))

# ... fake Users and miz coins ...
user_ids = []
user_in_log = []

for item in col_users.find({'coins': {'$exists': True}}, {'_id': 1}):
    user_ids.append(str(item['_id']))

for item in col_logs.distinct("user_id"):
    user_in_log.append(str(item))

fake_users = set(user_ids).intersection(set(user_in_log))
all_normal_users = set(user_ids).difference(set(fake_users))


all_registered_user = []
for item in col_users.find({},{'_id':1}):
    all_registered_user.append(item['_id'])

phone_register_users = []
for item in col_users.find({'code': '+98'}, {'_id': 1}):
    phone_register_users.append(str(item['_id']))

email_register_users = []
for item in col_users.find({'code': {'$ne': '+98'}}, {'_id': 1}):

    email_register_users.append(str(item['_id']))

phone_register_has_service = []
email_register_has_service = []
for item in service_users_ids:
    if str(item) in phone_register_users:
        phone_register_has_service.append(item)
    elif str(item) in email_register_users:
        email_register_has_service.append(item)

service_users_ids_set = set(service_users_ids)

has_no_service_phone_register = []
for item in phone_register_users:
    if ObjectId(item) not in service_users_ids_set:
        has_no_service_phone_register.append(ObjectId(item))

has_no_service_email_register = []
for item in email_register_users:
    if ObjectId(item) not in service_users_ids_set:
        has_no_service_email_register.append(ObjectId(item))

normal_service_user_phone_register = []
for item in col_services.aggregate([
    {'$match': {'$and': [{'parent_id': {'$exists': False}}, {'user.name': {'$ne': 'Admin'}},
                         {'user.id': {'$in': phone_register_users}}]}},
    {'$group': {'_id': '$user.id'}}

]):
    normal_service_user_phone_register.append(ObjectId(item['_id']))

complex_service_user_phone_register = []
for item in col_services.aggregate([
    {'$match': {'$and': [{'parent_id': {'$exists': True}}, {'user.name': {'$ne': 'Admin'}},
                         {'user.id': {'$in': phone_register_users}}]}},
    {'$group': {'_id': '$user.id'}}

]):
    complex_service_user_phone_register.append(ObjectId(item['_id']))

normal_service_user_email_register = []
for item in col_services.aggregate([
    {'$match': {'$and': [{'parent_id': {'$exists': False}}, {'user.name': {'$ne': 'Admin'}},
                         {'user.id': {'$in': email_register_users}}]}},
    {'$group': {'_id': '$user.id'}}

]):
    normal_service_user_email_register.append(ObjectId(item['_id']))

complex_service_user_email_register = []
for item in col_services.aggregate([
    {'$match': {'$and': [{'parent_id': {'$exists': True}}, {'user.name': {'$ne': 'Admin'}},
                         {'user.id': {'$in': email_register_users}}]}},
    {'$group': {'_id': '$user.id'}}

]):
    complex_service_user_email_register.append(ObjectId(item['_id']))

"""
Less Than 1 second
"""
irancell_users = []
mci_users = []
rightel_users = []
talia_users = []
irancell = ['935', '936', '937', '938', '939', '930', '933', '901', '902', '903', '905']
for item in col_users.find({'fake': {'$exists': False}}, {'mobile': 1}):
    if 'mobile' in item and item['mobile'] != "":
        if item['mobile'][:3] in irancell:
            irancell_users.append(item['_id'])
        elif item['mobile'][:3] == '922':
            rightel_users.append(item['_id'])
        elif item['mobile'][:3] == '932':
            talia_users.append(item['_id'])
        else:
            mci_users.append(item['_id'])
normal_users_with_refcode = []
for item in col_users.find({'referer': {'$ne': ''}},{'_id':1}):
    normal_users_with_refcode.append(item['_id'])
# print(1)
normal_users_without_refcode = []
for item in col_users.find({'referer': ''},{'_id':1}):
    normal_users_without_refcode.append(item['_id'])
# print(2)
normal_users_complete_profile = []
for item in col_users.find({'medals.group': 'profile'},{'_id':1}):
    normal_users_complete_profile.append(item['_id'])
# print(3)
normal_users_no_complete_profile = []
for item in col_users.find({'medals': []},{'_id':1}):
    normal_users_no_complete_profile.append(item['_id'])

final_report_dashboard = {
                    'date': datetime.now(),
                    'all_users': all_user,
                    'all_registered_users': len(all_registered_user),
                    'guest_users': all_user - len(all_normal_users),
                    'normal_users': len(all_normal_users), ##########
                    'normal_users_with_refcode': len(normal_users_with_refcode),
                    'normal_users_without_refcode': len(normal_users_without_refcode),
                    'normal_users_complete_profile':len(normal_users_complete_profile) ,
                    'normal_users_no_complete_profile': len(normal_users_no_complete_profile),
                    'user_register_with_number': len(phone_register_users), ##########
                    'sim_cards_operators': {
                        'MCI-IR': len(mci_users),
                        'Irancell': len(irancell_users),
                        'RighTel': len(rightel_users),
                        'Taliya': len(talia_users)
                    },
                    'user_register_without_number': len(email_register_users),##########
                    'phone_register_has_service': len(phone_register_has_service),##########
                    'email_register_has_service': len(email_register_has_service),##########
                    'phone_register_has_no_service': len(has_no_service_phone_register),
                    'email_register_has_no_service': len(has_no_service_email_register),
                    'no_complex_service_with_phone_register': len(normal_service_user_phone_register),
                    'no_complex_service_with_email_register': len(normal_service_user_email_register),
                    'complex_service_with_phone_register': len(complex_service_user_phone_register),
                    'complex_service_with_email_register': len(complex_service_user_email_register)
                }

col_dashboard_report.insert_one(final_report_dashboard)


all_normal_users2 = []
for item in all_normal_users:
    all_normal_users2.append(ObjectId(item))

phone_register_users2 = []
for item in phone_register_users:
    phone_register_users2.append(ObjectId(item))

email_register_users2 = []
for item in email_register_users:
    email_register_users2.append(ObjectId(item))

phone_register_has_service2 = []
for item in phone_register_has_service:
    phone_register_has_service2.append(ObjectId(item))

email_register_has_service2 = []
for item in email_register_has_service:
    email_register_has_service2.append(ObjectId(item))

report = {
    'alu': all_registered_user,
    'nu': all_normal_users2,
    'nuwr': normal_users_with_refcode,
    'nuwur': normal_users_without_refcode,
    'nucp': normal_users_complete_profile,
    'nuncp': normal_users_no_complete_profile,
    'urwn': phone_register_users2,
    'simir':mci_users,
    'simirn': irancell_users,
    'simright':rightel_users,
    'simta':talia_users, #####
    'urwun': email_register_users2, #####
    'prhs': phone_register_has_service2,
    'erhs': email_register_has_service2, #####
    'prhns': has_no_service_phone_register,
    'erhns': has_no_service_email_register, #####
    'ncsp': normal_service_user_phone_register,
    'ncse': normal_service_user_email_register, #####
    'csp': complex_service_user_phone_register, #####
    'cse': complex_service_user_email_register #####
}


check_time = datetime.now() - timedelta(days=amount)

check_time = datetime.now() - timedelta(days=amount * 7)

check_time = datetime.now() - timedelta(days=amount * 30)