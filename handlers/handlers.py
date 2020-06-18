from bson.json_util import loads
from base_handler import BaseHandler
from publics import PrintException, create_md5, encode_token, db
from datetime import datetime , timedelta
from bson import ObjectId
import math
import jdatetime


class Register(BaseHandler):
    def init_method(self):
        self.module = 'users'
        self.tokenless = True
        self.required = {
            'post': ['username', 'password', 'name', 'family', 'role']
        }

    def before_post(self):
        try:
            col_users = db()['users']
            if col_users.find_one({'username': self.params['username']}):
                self.set_output('user', 'user_exist')
                return False
            else:
                self.params['password'] = create_md5(self.params['password'])
        except:
            PrintException()
            return False
        return True


class Login(BaseHandler):
    def init_method(self):

        self.required = {
            'post': ['mobile', 'password']
        }
        self.inputs = {
            'post': ['mobile', 'password'],
        }
        self.tokenless = True

    def before_post(self):

        try:
            # print('yessssss22222')
            col_users = self.db['users']
            user_info = col_users.find_one({'mobile': self.params['mobile'],
                                            'password': create_md5(self.params['password'])})
            print(user_info)
            if user_info is not None:
                # print('start login')
                self.user_id = str(user_info['_id'])
                self.user_role = str(user_info['role'])
                self.token = encode_token({'user_id': self.user_id, 'role': self.user_role}).decode('ascii')
                self.output['token'] = self.token
                self.set_output('user', 'logged_in')
                # print(self.token)
            else:
                self.set_output('user', 'login_failed')
        except:
            # print('noooooooooo')
            PrintException()
            return False
        self.allow_action = False
        return True


class Collect(BaseHandler):
    def init_method(self):
        self.module = 'logs'
        self.casting['dates']=['date']

    def before_post(self):
        try:
            params = loads(self.params['params'])
            col_collect = db()['logs']
            col_collect.insert_many(params)
            self.allow_action = False
        except:
            PrintException()
            return False
        return True


class CustomLog(BaseHandler):
    def init_method(self):
        self.casting['dates'] = ['date', 'from_date', 'to_date']
        self.casting['floats'] = ['duration']
        self.casting['dict'] = ['doc', 'original_params', 'params', 'details', 'output', '']

    def before_get(self):
            try:

                col_logs = db()['logs']
                col_users = db()['users']

                ip_list = []
                url_list = []
                userid_list = []
                duration = []
                users_per_ip = []
                error_info = []
                first_time = datetime.now()
                one_months_ago = datetime.now() - timedelta(days=30)
                first_1398 = datetime.strptime('2019-03-21', "%Y-%m-%d")
                three_months_ago = datetime.now() - timedelta(days=90)

                # When Have from_date and to_date
                # TODO: It's Logs Collection Try to Improve Performance Querying
                # ======================================================

                if 'from_date' in self.params and 'to_date' in self.params:
                    self.params['from_date'] = datetime.strptime(self.params['from_date'], "%Y/%m/%d %H:%M:%S")
                    self.params['to_date'] = datetime.strptime(self.params['to_date'], "%Y/%m/%d %H:%M:%S")
                    match_conditions = {'$match': {'date': {'$gt': self.params['from_date'],
                                                            '$lt': self.params['to_date']}}}

                    # start getting users per ip
                    if ('type' not in self.params) or ('type' in self.params and 'user_per_ip' in self.params['type']):
                        aggregate = []
                        aggregate.append({'$group': {'_id': {'ip': '$ip'}, 'user_id': {'$addToSet': '$user_id'}}})
                        aggregate.append({'$unwind': '$user_id'})
                        aggregate.append({'$group': {'_id': '$_id', 'user_id_count': {'$sum': 1}}})
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])*5})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['ip'] = item['_id']['ip']
                            item['users_count'] = item['user_id_count']
                            del item['user_id_count']
                            del item['_id']
                            users_per_ip.append(item)
                        users_per_ip = sorted(users_per_ip, key=lambda i: i['users_count'], reverse=True)
                    # end of getting users per ip

                    # start getting ip list
                    if ('type' not in self.params) or ('type' in self.params and 'ips' in self.params['type']):
                        aggregate = []
                        aggregate.append({"$group": {"_id": "$ip", "ip": {"$first": "$ip"}, "count": {"$sum": 1}}})
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])*5})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})

                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            ip_list.append(item)
                        ip_list = sorted(ip_list, key=lambda i: i['page_size'], reverse=True)
                    # end of getting ip lists

                    # start getting url lists
                    if ('type' not in self.params) or ('type' in self.params and 'url' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$url", "url": {"$first": "$url"}, "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])*5})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})

                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            url_list.append(item)
                        url_list = sorted(url_list, key=lambda i: i['page_size'], reverse=True)
                    # end of getting url lists

                    # start getting user id list
                    if ('type' not in self.params) or ('type' in self.params and 'user_id' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$user_id", "user_id": {"$first": "$user_id"},
                                                       "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])*5})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            if item['user_id'] is not None and item['user_id'] is not "":
                                user = col_users.find_one({'_id': ObjectId(item['user_id'])})
                                if user is not None:
                                    item['name'] = user['name']
                                    item['family'] = user['family']
                                    item['mobile'] = user['mobile']
                            del item['_id']
                            userid_list.append(item)
                        userid_list = sorted(userid_list, key=lambda i: i['count'], reverse=True)
                    # end of getting user id list

                    # start getting duration by module
                    if ('type' not in self.params) or ('type' in self.params and 'duration' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': '$module',"module": {"$first": "$module"},
                                                       'duration': {'$sum': '$duration'}, 'count': {'$sum': 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])*5})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['count'])
                            item['duration'] = round(item['duration'])
                            duration.append(item)
                        duration = sorted(duration, key=lambda i: i['count'], reverse=True)
                    # end of getting duration by module

                    # start  getting error count
                    if ('type' not in self.params) or ('type' in self.params and 'errors' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': {'module': '$module', 'http_code': '$http_code'},
                                                       'error_count': {'$sum': 1},
                                                       'duration': {'$sum': '$duration'}}}
                        match_conditions = {'$match': {'$and': {{'date': {'$gt': self.params['from_date'],
                                                                          '$lt': self.params['to_date']}},
                                                                {'http_code': {'$gte': 400, '$lte': 500}}}}}
                        aggregate.append(match_conditions)
                        aggregate.append(group_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'error_count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'error_count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['status_code'] = item['_id']['http_code']
                            item['module'] = item['_id']['module']
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['error_count'])
                            item['duration'] = round(item['duration'])
                            error_info.append(item)
                        if 'error_sort' in self.params and self.params['error_sort'] == 'duration':
                            error_info = sorted(error_info, key=lambda i: i['duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'avg_duration':
                            error_info = sorted(error_info, key=lambda i: i['avg_duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'error_count':
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                        else:
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                    # end of getting error count

                # When Have from_date But Not to_date
                # ======================================================

                elif 'from_date' in self.params and 'to_date' not in self.params:
                    self.params['from_date'] = datetime.strptime(self.params['from_date'], "%Y/%m/%d %H:%M:%S")
                    match_conditions = {'$match': {'date': {'$gte': self.params['from_date']}}}

                    # start getting users per ip
                    if ('type' not in self.params) or ('type' in self.params and 'user_per_ip' in self.params['type']):
                        aggregate = []
                        aggregate.append({'$group': {'_id': {'ip': '$ip'}, 'user_id': {'$addToSet': '$user_id'}}})
                        aggregate.append({'$unwind': '$user_id'})
                        aggregate.append({'$group': {'_id': '$_id', 'user_id_count': {'$sum': 1}}})
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['ip'] = item['_id']['ip']
                            item['users_count'] = item['user_id_count']
                            del item['user_id_count']
                            del item['_id']
                            users_per_ip.append(item)
                        users_per_ip = sorted(users_per_ip, key=lambda i: i['users_count'], reverse=True)
                    # end of getting users per ip

                    # start getting ip list
                    if ('type' not in self.params) or ('type' in self.params and 'ips' in self.params['type']):
                        aggregate = []
                        aggregate.append({"$group": {"_id": "$ip", "ip": {"$first": "$ip"}, "count": {"$sum": 1}}})
                        aggregate.append({'$group': {'_id': {'ip': '$ip'}, 'user_id': {'$addToSet': '$user_id'}}})
                        aggregate.append({'$unwind': '$user_id'})
                        aggregate.append({'$group': {'_id': '$_id', 'user_ids': {'$sum': 1}}})
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            ip_list.append(item)
                        ip_list = sorted(ip_list, key=lambda i: i['count'], reverse=True)
                    # end of getting ip lists

                    # start getting url lists
                    if ('type' not in self.params) or ('type' in self.params and 'ips' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$url", "url": {"$first": "$url"}, "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            url_list.append(item)
                        url_list = sorted(url_list, key=lambda i: i['count'], reverse=True)
                    # end of getting url lists

                    # start getting user id list
                    if ('type' not in self.params) or ('type' in self.params and 'user_id' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$user_id", "user_id": {"$first": "$user_id"},
                                                       "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            if item['user_id'] is not None and item['user_id'] is not "":
                                user = col_users.find_one({'_id': ObjectId(item['user_id'])})
                                if user is not None:
                                    item['name'] = user['name']
                                    item['family'] = user['family']
                                    item['mobile'] = user['mobile']
                            del item['_id']
                            userid_list.append(item)
                        userid_list = sorted(userid_list, key=lambda i: i['count'], reverse=True)
                    # end of getting user id list

                    # start getting duration by module
                    if ('type' not in self.params) or ('type' in self.params and 'duration' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': '$module', "module": {"$first": "$module"},
                                                       'duration': {'$sum': '$duration'}, 'count': {'$sum': 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['count'])
                            item['duration'] = round(item['duration'])
                            duration.append(item)
                        duration = sorted(duration, key=lambda i: i['count'], reverse=True)
                    # end of getting duration by module

                    # start  getting error count
                    if ('type' not in self.params) or ('type' in self.params and 'errors' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': {'module': '$module', 'http_code': '$http_code'},
                                                       'error_count': {'$sum': 1},
                                                       'duration': {'$sum': '$duration'}}}
                        match_conditions = {'$match': {'$and': {{'date': {'$gte': self.params['from_date']}},
                                                                {'http_code': {'$gte': 400, '$lte': 500}}}}}
                        aggregate.append(match_conditions)
                        aggregate.append(group_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'error_count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'error_count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['status_code'] = item['_id']['http_code']
                            item['module'] = item['_id']['module']
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['error_count'])
                            item['duration'] = round(item['duration'])
                            error_info.append(item)
                        if 'error_sort' in self.params and self.params['error_sort'] == 'duration':
                            error_info = sorted(error_info, key=lambda i: i['duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'avg_duration':
                            error_info = sorted(error_info, key=lambda i: i['avg_duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'error_count':
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                        else:
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                    # end of getting error count

                # When Doesn't Have from_date But to_date Yes
                # ======================================================

                elif 'to_date' in self.params and 'from_date' not in self.params:
                    self.params['to_date'] = datetime.strptime(self.params['to_date'], "%Y/%m/%d %H:%M:%S")
                    match_conditions = {'$match': {'date': {'$lte': self.params['to_date']}}}

                    # start getting users per ip
                    if ('type' not in self.params) or ('type' in self.params and 'user_per_ip' in self.params['type']):
                        aggregate = []
                        aggregate.append({'$group': {'_id': {'ip': '$ip'}, 'user_id': {'$addToSet': '$user_id'}}})
                        aggregate.append({'$unwind': '$user_id'})
                        aggregate.append({'$group': {'_id': '$_id', 'user_id_count': {'$sum': 1}}})
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['ip'] = item['_id']['ip']
                            item['users_count'] = item['user_id_count']
                            del item['user_id_count']
                            del item['_id']
                            users_per_ip.append(item)
                        users_per_ip = sorted(users_per_ip, key=lambda i: i['users_count'], reverse=True)
                    # end of getting users per ip

                    # start getting ip list
                    if ('type' not in self.params) or ('type' in self.params and 'ips' in self.params['type']):
                        aggregate = []
                        aggregate.append({"$group": {"_id": "$ip", "ip": {"$first": "$ip"}, "count": {"$sum": 1}}})
                        aggregate.append({'$group': {'_id': {'ip': '$ip'}, 'user_id': {'$addToSet': '$user_id'}}})
                        aggregate.append({'$unwind': '$user_id'})
                        aggregate.append({'$group': {'_id': '$_id', 'user_ids': {'$sum': 1}}})
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            ip_list.append(item)
                        ip_list = sorted(ip_list, key=lambda i: i['count'], reverse=True)
                    # end of getting ip lists

                    # start getting url lists
                    if ('type' not in self.params) or ('type' in self.params and 'url' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$url", "url": {"$first": "$url"}, "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            url_list.append(item)
                        url_list = sorted(url_list, key=lambda i: i['count'], reverse=True)
                    # end of getting url lists

                    # start getting user id list
                    if ('type' not in self.params) or ('type' in self.params and 'user_id' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$user_id", "user_id": {"$first": "$user_id"},
                                                       "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            if item['user_id'] is not None and item['user_id'] is not "":
                                user = col_users.find_one({'_id': ObjectId(item['user_id'])})
                                if user is not None:
                                    item['name'] = user['name']
                                    item['family'] = user['family']
                                    item['mobile'] = user['mobile']
                            del item['_id']
                            userid_list.append(item)
                        userid_list = sorted(userid_list, key=lambda i: i['count'], reverse=True)
                    # end of getting user id list

                    # start getting duration by module
                    if ('type' not in self.params) or ('type' in self.params and 'duration' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': '$module', "module": {"$first": "$module"},
                                                       'duration': {'$sum': '$duration'}, 'count': {'$sum': 1}}}
                        aggregate.append(group_conditions)
                        aggregate.append(match_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$skip': int(self.params['page'])})
                        if 'page_size' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['count'])
                            item['duration'] = round(item['duration'])
                            duration.append(item)
                        duration = sorted(duration, key=lambda i: i['count'], reverse=True)
                    # end of getting duration by module

                    # start  getting error count
                    if ('type' not in self.params) or ('type' in self.params and 'errors' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': {'module': '$module', 'http_code': '$http_code'},
                                                       'error_count': {'$sum': 1},
                                                       'duration': {'$sum': '$duration'}}}
                        match_conditions = {'$match': {'$and': {{'date': {'$lte': self.params['to_date']}},
                                                                {'http_code': {'$gte': 400, '$lte': 500}}}}}
                        aggregate.append(match_conditions)
                        aggregate.append(group_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'error_count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'error_count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['status_code'] = item['_id']['http_code']
                            item['module'] = item['_id']['module']
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['error_count'])
                            item['duration'] = round(item['duration'])
                            error_info.append(item)
                        if 'error_sort' in self.params and self.params['error_sort'] == 'duration':
                            error_info = sorted(error_info, key=lambda i: i['duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'avg_duration':
                            error_info = sorted(error_info, key=lambda i: i['avg_duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'error_count':
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                        else:
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                    # end of getting error count
                
                # When Doesn't have from_date and to_date
                # ======================================================

                elif 'from_date' not in self.params and 'to_date' not in self.params:

                    # start getting users per ip
                    if ('type' not in self.params) or ('type' in self.params and 'user_per_ip' in self.params['type']):
                        aggregate = []
                        aggregate.append({'$group': {'_id': {'ip': '$ip'}, 'user_id': {'$addToSet': '$user_id'}}})
                        aggregate.append({'$unwind': '$user_id'})
                        aggregate.append({'$group': {'_id': '$_id', 'user_id_count': {'$sum': 1}}})
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'user_id_count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'user_id_count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['ip'] = item['_id']['ip']
                            item['users_count'] = item['user_id_count']
                            del item['user_id_count']
                            del item['_id']
                            users_per_ip.append(item)
                        if 'page_size' not in self.params:
                            users_per_ip = sorted(users_per_ip, key=lambda i: i['users_count'], reverse=True)
                    # end of getting users per ip

                    # start getting ip list
                    if ('type' not in self.params) or ('type' in self.params and 'ips' in self.params['type']):
                        aggregate = []
                        aggregate.append({"$group": {"_id": "$ip", "ip": {"$first": "$ip"}, "count": {"$sum": 1}}})
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            ip_list.append(item)
                        if 'page_size' not in self.params:
                            ip_list = sorted(ip_list, key=lambda i: i['count'], reverse=True)
                    # end of getting ip lists

                    # start getting url lists
                    if ('type' not in self.params) or ('type' in self.params and 'url' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$url", "url": {"$first": "$url"}, "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        # aggregate.append({'allowDiskUse': 'true'})
                        for item in col_logs.aggregate(aggregate, allowDiskUse=True):
                            del item['_id']
                            url_list.append(item)
                        if 'page_size' not in self.params:
                            url_list = sorted(url_list, key=lambda i: i['count'], reverse=True)
                    # end of getting url lists

                    # start getting user id list
                    if ('type' not in self.params) or ('type' in self.params and 'user_id' in self.params['type']):
                        aggregate = []
                        group_conditions = {"$group": {"_id": "$user_id", "user_id": {"$first": "$user_id"},
                                                       "count": {"$sum": 1}}}
                        aggregate.append(group_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            if item['user_id'] is not None and item['user_id'] is not "":
                                user = col_users.find_one({'_id': ObjectId(item['user_id'])})
                                if user is not None:
                                    item['name'] = user['name']
                                    item['family'] = user['family']
                                    item['mobile'] = user['mobile']
                            del item['_id']
                            userid_list.append(item)
                        if 'page_size' not in self.params:
                            userid_list = sorted(userid_list, key=lambda i: i['count'], reverse=True)
                    # end of getting user id list

                    # start getting duration by module
                    if ('type' not in self.params) or ('type' in self.params and 'duration' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': '$module', "module": {"$first": "$module"},
                                                       'duration': {'$sum': '$duration'}, 'count': {'$sum': 1}}}
                        aggregate.append(group_conditions)

                        if 'page' in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$skip': int(self.params['page'])*int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['count'])
                            item['duration'] = round(item['duration'])
                            duration.append(item)
                        duration = sorted(duration, key=lambda i: i['avg_duration'], reverse=True)
                    # end of getting duration by module

                    # start  getting error count
                    if ('type' not in self.params) or ('type' in self.params and 'errors' in self.params['type']):
                        aggregate = []
                        group_conditions = {'$group': {'_id': {'module': '$module', 'http_code': '$http_code'},
                                                       'error_count': {'$sum': 1}, 'duration': {'$sum': '$duration'}}}
                        match_conditions = {'$match': {'http_code': {'$gte': 400, '$lte': 500}}}
                        aggregate.append(match_conditions)
                        aggregate.append(group_conditions)
                        if 'page' in self.params:
                            aggregate.append({'$sort': {'error_count': -1}})
                            aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                        if 'page_size' in self.params:
                            if 'page' in self.params:
                                aggregate.append({'$limit': int(self.params['page_size'])})
                            elif 'page' not in self.params:
                                aggregate.append({'$sort': {'error_count': -1}})
                                aggregate.append({'$limit': int(self.params['page_size'])})
                        for item in col_logs.aggregate(aggregate):
                            item['status_code'] = item['_id']['http_code']
                            item['module'] = item['_id']['module']
                            del item['_id']
                            item['avg_duration'] = round(item['duration'] / item['error_count'])
                            item['duration'] = round(item['duration'])
                            error_info.append(item)
                        if 'error_sort' in self.params and self.params['error_sort'] == 'duration':
                            error_info = sorted(error_info, key=lambda i: i['duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'avg_duration':
                            error_info = sorted(error_info, key=lambda i: i['avg_duration'], reverse=True)

                        elif 'error_sort' in self.params and self.params['error_sort'] == 'error_count':
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                        else:
                            error_info = sorted(error_info, key=lambda i: i['error_count'], reverse=True)
                    # end of getting error count
                # ======================================================

                """ 
                
                get user ids from services
                
                """
                col_services = db()['services']
                col_search = db()['search_history']
                service_ids = []
                for service_id in col_services.find({}, {'user.id': 1}):
                    if 'user' in service_id:
                        service_ids.append(service_id['user']['id'])
                """ 
                end of line 
                
                """
                # ======================================================
                # start getting services who send request
                services_search = []
                if ('type' not in self.params) or ('type' in self.params and 'service_search' in self.params['type']):

                    aggregate = []
                    aggregate.append({'$match': {'user_id': {'$in': service_ids}}})
                    aggregate.append({'$group': {'_id': '$user_id', 'count': {'$sum': 1}}})
                    if 'page' in self.params:
                        aggregate.append({'$sort': {'count': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for check in col_search.aggregate(aggregate):
                        services_search.append(check)
                # end of getting services who send request
                # ======================================================

                # ======================================================

                # start getting last update services in 3 month ago

                threemonth_lastupdate_services = []
                if ('type' not in self.params) or ('type' in self.params and 'service_3month' in self.params['type']):

                    aggregate = []
                    aggregate.append({'$match': {'last_update': {'$lte': datetime.now(), '$gte': three_months_ago}}})
                    aggregate.append({'$group': {'_id': '$_id', 'phone': {'$first': '$phone'},
                                                 'city_name': {'$first': '$city_name'}, 'title': {'$first': '$title'}}})
                    if 'page' in self.params:
                        aggregate.append({'$sort': {'user_id_count': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'user_id_count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for item in col_services.aggregate(aggregate):
                        item['service_id'] = str(item['_id'])
                        del item['_id']
                        threemonth_lastupdate_services.append(item)
                # end of getting last update services in 3 month ago

                # ======================================================

                # TODO: It's Logs Collection Try to Improve Performance Querying
                # start getting active services
                active_service_complex = []
                active_service_no_complex = []
                if ('type' not in self.params) or ('type' in self.params and 'active_services' in self.params['type']):
                    with_complex = []
                    no_complex = []
                    one_month = 30
                    selected_month = 1
                    agg_group = {'$group': {'_id': '$user_id', 'user_id_count': {'$sum': 1}}}
                    custom_month = datetime.now() - timedelta(days=selected_month * one_month)

                    for service_id in col_services.find(
                            {'$and': [{'parent_id': {'$exists': True}}, {'parent_id': {'$ne': ''}}]}, {'user.id': 1}):
                        with_complex.append(service_id['user']['id'])

                    for service_id in col_services.find({'$or': [{'parent_id': {'$exists': False}}, {'parent_id': ''}]},
                                                        {'user.id': 1}):
                        if 'user' in service_id and service_id['user'] is not None:
                            no_complex.append(service_id['user']['id'])

                    aggregate = []
                    aggregate.append({'$match': {'$and': [{'user_id': {'$in': with_complex}},
                                                          {'date': {'$lte': datetime.now(), '$gte':custom_month}}]}})
                    aggregate.append(agg_group)
                    if 'page' in self.params:
                        aggregate.append({'$sort': {'user_id_count': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'user_id_count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for item in col_logs.aggregate(aggregate):
                        item['user_id'] = item['_id']
                        del item['_id']
                        active_service_complex.append(item)

                    aggregate = []
                    aggregate.append({'$match': {'$and': [{'user_id': {'$in': no_complex}},
                                             {'date': {'$lte': datetime.now(), '$gte': custom_month}}]}})
                    aggregate.append(agg_group)

                    if 'page' in self.params:
                        aggregate.append({'$sort': {'user_id_count': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'user_id_count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for item in col_logs.aggregate(aggregate):
                        item['user_id'] = item['_id']
                        del item['_id']
                        active_service_no_complex.append(item)

                # end of getting active services

                # ======================================================

                # TODO: It's Logs Collection Try to Improve Performance Querying
                # start getting users info per user id by ip
                if 'ip' in self.params:
                    user_ids = []
                    users_info = []
                    for item in col_logs.find({'ip': self.params['ip']}):
                        if item['user_id'] not in user_ids:
                            user_ids.append(item['user_id'])
                            user_info = col_users.find_one({'_id': ObjectId(item['user_id'])})
                            if user_info is not None:
                                users_info.append({
                                    'user_id': item['user_id'],
                                    'name': user_info['name'],
                                    'family': user_info['family'],
                                    'mobile': user_info['mobile']
                                })
                    self.output['data']['item'] = users_info
                # end of getting users info per user id by ip

                # ======================================================

                # create average of search history in per day period
                exit_search_avg = ""
                if ('type' not in self.params) or ('type' in self.params and 'average_search' in self.params['type']):
                    col_search = db()['search_history']
                    count = 0
                    search_list = []
                    for item in col_search.aggregate([
                        {'$match': {'create_date': {'$gte': first_1398}}},
                        {'$group': {'_id': '$avg_date', 'avg_date': {'$first': '$avg_date'},
                                    'count': {'$sum': 1}}}
                    ]):
                        count += item['count']
                        search_list.append(item)
                    exit_search_avg = math.ceil(count / len(search_list))

                # end creating average of search history in per day period

                # ======================================================

                # create average of click on call or direction(all and per day period)
                call_avg = ""
                direction_avg = ""
                if ('type' not in self.params) or ('type' in self.params and 'event_avg' in self.params['type']):
                    col_events = db()['events']

                    count = 0
                    event_list = []
                    for item in col_events.aggregate([
                        {'$match': {'$and': [{'name': 'direction'}, {'create_date': {'$gte': first_1398}}]}},
                        {'$group': {'_id': '$avg_date', 'count': {'$sum': 1}}}
                    ]):
                        count += item['count']
                        event_list.append(item)
                    direction_avg = math.ceil(count / len(event_list))

                    count = 0
                    event_list = []
                    for item in col_events.aggregate([
                        {'$match': {'$and': [{'name': 'call'}, {'create_date': {'$gte': first_1398}}]}},
                        {'$group': {'_id': '$avg_date', 'count': {'$sum': 1}}}
                    ]):
                        count += item['count']
                        event_list.append(item)
                    call_avg = math.ceil(count / len(event_list))

                # end creating average of click on call or direction(all and per day period)

                # ======================================================

                # create average of chat in per day period
                chat_avg = ""
                if ('type' not in self.params) or ('type' in self.params and 'chat_avg' in self.params['type']):

                    col_chat = db()['chat']

                    count = 0
                    chat_list = []
                    for item in col_chat.aggregate([
                        {'$match': {'create_date': {'$gte': first_1398}}},
                        {'$group': {'_id': '$avg_date', 'avg_date': {'$first': '$avg_date'}, 'count': {'$sum': 1}}}
                    ]):
                        count += item['count']
                        chat_list.append(item)
                    chat_avg = math.ceil(count / len(chat_list))
                # end creating average of chat in per day period

                # ======================================================

                # start getting services per city name
                service_city_list = []
                if ('type' not in self.params) or ('type' in self.params and 'service_per_city' in self.params['type']):

                    aggregate = []
                    aggregate.append({'$match': {'user.name': {'$ne': 'Admin'}}})

                    aggregate.append({'$group': {'_id': '$city_name', 'count': {'$sum': 1}}})
                    if 'page' in self.params:
                        aggregate.append({'$sort': {'count': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for item in col_services.aggregate(aggregate):
                        service_city_list.append(item)
                # end of getting services per city name

                # ======================================================

                # start getting users of cities
                users_cities = []
                if ('type' not in self.params) or ('type' in self.params and 'user_cities' in self.params['type']):

                    aggregate = []
                    aggregate.append({'$match': {'user.name': {'$ne': 'Admin'}}})
                    aggregate.append({'$group': {'_id': '$city_name', 'count': {'$sum': 1}}})
                    if 'page' in self.params:
                        aggregate.append({'$sort': {'count': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for item in col_users.aggregate(aggregate):
                        users_cities.append(item)
                # end of getting users of cities

                # ======================================================

                # TODO: It's Logs Collection Try to Improve Performance Querying
                # start to find fake users
                fake_users_id = []
                exit_fake_users = []
                coins = 0
                if ('type' not in self.params) or ('type' in self.params and 'fake_users' in self.params['type']):
                    user_ids = []
                    user_in_log = []

                    for item in col_users.find({'coins': {'$exists': True}}, {'_id': 1}):
                        user_ids.append(str(item['_id']))

                    for item in col_logs.aggregate([
                        {'$group': {'_id': '$user_id'}}
                    ]):
                        user_in_log.append(item['_id'])

                    for item in user_ids:
                        if item not in user_in_log:
                            fake_users_id.append(item)
                            user = col_users.find_one({'_id': ObjectId(item)}, {'coins': 1})
                            coins += user['coins']
                    exit_fake_users = {
                        'fake_users': len(fake_users_id),
                        'miz_coins': coins
                    }
                # end of finding fake users

                # ======================================================

                # TODO: It's Logs Collection Try to Improve Performance Querying
                # start to finding active users
                active_user_ids = []
                if ('type' not in self.params) or ('type' in self.params and 'active_users' in self.params['type']):

                    for item in col_logs.aggregate([
                        {'$match': {'date': {'$lte': datetime.now(), '$gte': one_months_ago}}},
                        {'$group': {'_id': '$user_id'}}
                    ]):
                        active_user_ids.append(item)
                # end to finding active users


                # ======================================================

                # TODO: It's Logs Collection Try to Improve Performance Querying
                # start to finding normal users
                normal_user_ids = []
                if ('type' not in self.params) or ('type' in self.params and 'normal_users' in self.params['type']):

                    for item in col_logs.aggregate([
                        {'$match': {'date': {'$lt': one_months_ago}}},
                        {'$group': {'_id': '$user_id'}}
                    ]):
                        normal_user_ids.append(item)
                # end to finding de active users

                # ======================================================

                # start sorting services per cities with parent id
                per_cities_complex =[]
                if ('type' not in self.params) or ('type' in self.params and 'complex_services_cities' in self.params['type']):

                    aggregate = []
                    aggregate.append({'$match': {'$and': [{'parent_id': {'$exists': True}}, {'parent_id': {'$ne': ''}},
                                                          {'user.name': {'$ne': 'Admin'}}]}})
                    aggregate.append({'$group': {'_id': '$city_name', 'count': {'$sum': 1}}})
                    if 'page' in self.params:
                        aggregate.append({'$sort': {'coun': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for item in col_services.aggregate(aggregate):
                        per_cities_complex.append(item)
                # end sorting services per cities with parent id

                # ======================================================

                # start sorting services per cities without parent id
                per_cities_no_complex = []
                if ('type' not in self.params) or (
                        'type' in self.params and 'no_complex_services_cities' in self.params['type']):

                    aggregate = []
                    aggregate.append({'$match': {'$and': [{'$or': [{'parent_id': {'$exists': False}},
                                                                   {'parent_id': ''}]},
                                                          {'user.name': {'$ne': 'Admin'}}]}})
                    aggregate.append({'$group': {'_id': '$city_name', 'count': {'$sum': 1}}})
                    if 'page' in self.params:
                        aggregate.append({'$sort': {'count': -1}})
                        aggregate.append({'$skip': int(self.params['page']) * int(self.params['page_size'])})
                    if 'page_size' in self.params:
                        if 'page' in self.params:
                            aggregate.append({'$limit': int(self.params['page_size'])})
                        elif 'page' not in self.params:
                            aggregate.append({'$sort': {'count': -1}})
                            aggregate.append({'$limit': int(self.params['page_size'])})

                    for item in col_services.aggregate(aggregate):
                        per_cities_no_complex.append(item)
                # end sorting services per cities without parent id

                # ======================================================

                log_query = [
                    {'users_per_ip': users_per_ip},
                    {'ips': ip_list},
                    {'urls': url_list},
                    {'user_ids': userid_list},
                    {'durations': duration},
                    {'error_info': error_info},
                    {'services_by_search': services_search},
                    {'services_lastUpdate_3month_ago': threemonth_lastupdate_services},
                    {'search_avg_perDay': exit_search_avg},
                    {'avg_direction': direction_avg},
                    {'avg_call': call_avg},
                    {'chat_avg': chat_avg},
                    {'fake_users': exit_fake_users},
                    {'users_of_cities': users_cities},
                    {'services_of_cities': service_city_list},
                    {'active_users': len(active_user_ids)},
                    {'normal_users': len(normal_user_ids)},
                    {'complex_services_cities': per_cities_complex},
                    {'no_complex_services_cities': per_cities_no_complex},
                ]

                self.output['data']['list'] = log_query
                last_time = datetime.now()
                print('the final time :', last_time-first_time)
            except:
                PrintException()
                return False
            self.allow_action = False
            self.set_output('public_operations', 'successful')
            return True


class Logs(BaseHandler):
    def init_method(self):
        self.casting['dict'] = ['doc', 'original_params', 'params', 'details', 'output', '']
        self.casting['dates'] = ['date', 'from_date', 'to_date']
        self.casting['floats'] = ['duration']
        self.casting['ints'] = ['http_code']
        self.casting['booleans'] = ['status']
        self.inputs = {
            'post': ['params', 'status', 'date', 'http_code', 'token', 'user_id', 'output', 'method', 'url', 'debug',
                     'details', 'result', 'doc', 'project', 'ip', 'duration', 'original_params']
                        }

    def before_get(self):
            try:
                if 'from_date' in self.params and 'to_date' in self.params:

                    print(type(self.params['from_date']))
                    self.params['from_date'] = datetime.strptime(self.params['from_date'], "%Y/%m/%d %H:%M:%S")
                    self.params['to_date'] = datetime.strptime(self.params['to_date'], "%Y/%m/%d %H:%M:%S")
                    self.conditions = {'date': {'$gt': self.params['from_date'], '$lt': self.params['to_date']}}
                    print(self.params['from_date'])

                elif 'from_date' in self.params and 'to_date' not in self.params:

                    self.params['from_date'] = datetime.strptime(self.params['from_date'], "%Y/%m/%d %H:%M:%S")
                    self.conditions = {'date': {'$gte': self.params['from_date']}}
                    print(self.params['from_date'])

                elif 'to_date' in self.params and 'from_date' not in self.params:

                    self.params['to_date'] = datetime.strptime(self.params['to_date'], "%Y/%m/%d %H:%M:%S")
                    self.conditions = {'date': {'$lte': self.params['to_date']}}
                    print(self.params['to_date'])
            except:
                PrintException()
                return False
            self.set_output('public_operations', 'successful')
            return True


class Dashboard(BaseHandler):
    def init_method(self):

        self.casting['dates'] = ['date']

    def before_get(self):
        try:
            print('start updating dashboard')
            time_now = datetime.now()
            col_dashboard_report = db()['dashboard']
            col_users = db()['users']
            col_services = db()['services']
            col_logs = db()['logs']

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

            t4 = datetime.now()

            for item in col_logs.distinct("user_id"):
                user_in_log.append(str(item))

            print(datetime.now() - t4)
            fake_users = set(user_ids).intersection(set(user_in_log))
            all_normal_users = set(user_ids).difference(set(fake_users))
            """
            fake_users_id = []
            user_in_log_set = set(user_in_log)
            for item in user_ids:
                if item not in user_in_log_set:
                    fake_users_id.append(ObjectId(item))

            coins = 0
            for item in col_users.aggregate([
                {'$match': {'$and': [{'_id': {'$in': fake_users_id}}, {'coins': {'$exists': True}}]}},
                {"$group": {'_id': 'null', 'coins': {'$sum': '$coins'}}}
            ]):
                coins = item['coins']
            """
            # ... end of fake users ...

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
            final_report_dashboard = {}
            if 'report' not in self.params:
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
                final_report_dashboard['date'] = str(final_report_dashboard['date'])
                del final_report_dashboard['_id']

            elif 'report' in self.params:
                print('start to specified by date ')
                results = {'message':'Sorry This Field Is Empty!'}

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

                col = self.db['users']
                # print('kooni user')
                # for item in col.find({'_id':{'$in':normal_users_complete_profile}}):
                #     if 'create_date' not in item:
                #         print(item)
                # print('end kooni user')
                amount = 10 if 'amount' not in self.params else int(self.params['amount'])
                types = 'daily' if 'type' not in self.params else self.params['type']

                if 'from_date' and 'to_date' in self.params:
                    self.params['from_date'] = datetime.strptime(self.params['from_date'], "%Y/%m/%d")
                    self.params['to_date'] = datetime.strptime(self.params['to_date'], "%Y/%m/%d")

                    match = {'$match': {
                            'create_date': {'$gte': self.params['from_date'],
                                            '$lte': self.params['to_date'] + timedelta(days=1)},
                            '_id': {'$in': report[self.params['report']]}
                        }}
                    cum_match = {'$match': {
                            'create_date': {'$lt': self.params['from_date']},
                            '_id': {'$in': report[self.params['report']]}
                        }}

                elif 'from_date' and 'to_date' not in self.params:
                    check_time = datetime.now() - timedelta(days=amount)
                    if types == 'daily':
                        check_time = datetime.now() - timedelta(days=amount)
                    elif types == 'weekly':
                        check_time = datetime.now() - timedelta(days=amount * 7)
                    elif types == 'monthly':
                        check_time = datetime.now() - timedelta(days=amount * 30)

                    match = {'$match': {
                            'create_date': {
                                '$gte': check_time},
                            '_id': {'$in': report[self.params['report']]}
                        }}
                    cum_match = {'$match': {
                            'create_date': {
                                '$lte': check_time},
                            '_id': {'$in': report[self.params['report']]}
                        }}

                if types == 'daily':

                    print('daily')

                    daily_project = {"$project":
                            {
                                "_id": 0,
                                "datePartDay": {"$concat": [
                                    {"$substr": [{"$year": "$create_date"}, 0, 4]}, "-",
                                    {"$substr": [{"$month": "$create_date"}, 0, 2]}, "-",
                                    {"$substr": [{"$dayOfMonth": "$create_date"}, 0, 2]}
                                ]}}
                        }
                    daily_group = {"$group":
                             {"_id": "$datePartDay", "count": {"$sum": 1}}
                         }
                    results = col.aggregate([
                        match,
                        daily_project,
                        daily_group
                    ])

                    cum_result = col.aggregate([
                        cum_match,
                        daily_project,
                        daily_group
                    ])
                    print(results)
                    print('end aggregate')

                elif types == 'weekly':
                    print('weekly')
                    weekly_project ={'$project': {
                            'year': {'$year': '$create_date'},
                            'week': {'$week': '$create_date'},
                        }}
                    weekly_group = {
                            '$group': {
                                '_id': {'year': '$year', 'week': '$week'}, 'count': {'$sum': 1}
                            }
                        }
                    results = col.aggregate([
                        match,
                        weekly_project,
                        weekly_group

                    ])
                    cum_result = col.aggregate([
                        cum_match,
                        weekly_project,
                        weekly_group

                    ])
                elif types == 'monthly':
                    print('monthly')
                    monthly_project = {'$project': {
                            'year': {'$year': '$create_date'},
                            'month': {'$month': '$create_date'},
                        }}
                    monthly_group = {
                            '$group': {
                                '_id': {'year': '$year', 'month': '$month'}, 'count': {'$sum': 1}
                            }
                        }
                    results = col.aggregate([
                        match,
                        monthly_project,
                        monthly_group
                    ])

                    cum_result = col.aggregate([
                        cum_match,
                        monthly_project,
                        monthly_group

                    ])

                    # print('yekkkkkk')
                print('enddddd')
                counter = 0
                cumulative = 0
                for item in cum_result:
                    cumulative += item['count']

                results_list = []

                # print(type(results))
                # print(list(results))
                for item in list(results):
                    item['date'] = item['_id']
                    del item['_id']
                    counter += item['count']
                    results_list.append(item)
                print(results_list)
                print(cumulative)
                print(counter)
                print('#')
                print('#')
                print('#')

                if types == 'daily':
                    for item in results_list:
                        item['date'] = datetime.strptime(item['date'],"%Y-%m-%d")
                        item['date'] = jdatetime.datetime.fromgregorian(datetime=item['date'])
                    results_list = sorted(results_list, key=lambda i: i['date'])
                    for item in results_list:
                            cumulative += item['count']
                            item['cumulative'] = cumulative
                    results_list = sorted(results_list, key=lambda i: i['date'], reverse=True)
                    for item in results_list:
                        item['date'] = str(item['date'])

                #

                elif types == 'weekly':
                    for item in results_list:
                        if item['date']['week'] < 10:
                            item['dating'] = int(str(item['date']['year'])+str(0) + str(item['date']['week']))

                        elif item['date']['week'] >= 10:
                            item['dating'] = int(str(item['date']['year'])+str(item['date']['week']))

                    results_list = sorted(results_list, key=lambda i: i['dating'])
                    for item in results_list:
                            cumulative += item['count']
                            item['cumulative'] = cumulative
                    results_list = sorted(results_list, key=lambda i: i['dating'], reverse=True)
                    for item in results_list:
                        if 'dating' in item:
                            del item['dating']

                #

                elif types == 'monthly':
                    for item in results_list:
                        if item['date']['month'] < 10:
                            item['dating'] = int(str(item['date']['year']) + str(0) + str(item['date']['month']))

                        elif item['date']['month'] >= 10:
                            item['dating'] = int(str(item['date']['year']) + str(item['date']['month']))
                        item['date'] = jdatetime.date.fromgregorian(day=1, month=item['date']['month'],
                                                                    year=item['date']['year'])
                        item['date'] = str(item['date'])
                        dte = item['date'].split('-')
                        # print(dte)
                        item['date'] = {'year':dte[0],
                                        'month':dte[1]}
                    results_list = sorted(results_list, key=lambda i: i['dating'])

                    for item in results_list:
                            cumulative += item['count']
                            item['cumulative'] = cumulative

                    results_list = sorted(results_list, key=lambda i: i['dating'], reverse=True)
                    for item in results_list:
                        if 'dating' in item:
                            del item['dating']

                    # print(type(results_list[0]['dating']))

                #

                self.output['data']['list'] = results_list
            self.set_output('public_operations', 'successful')
            end_time = datetime.now()
            print('Time Consumed: ', end_time - time_now)
            self.output['data']['item'] = final_report_dashboard
        except:
            PrintException()
            return False
        self.allow_action = False
        self.set_output('public_operations', 'successful')
        return True


class LastDashboard(BaseHandler):
    def init_method(self):
        self.casting['dates'] = ['date']
        self.casting['dict'] = ['doc', 'original_params', 'params', 'details', 'output', '']

    def before_get(self):
            try:
                time_now = datetime.now()
                final_report_dashboard = []
                col_dashboard = db()['dashboard']
                for item in col_dashboard.find().sort('date', -1).limit(1):
                    final_report_dashboard = {
                        'date': str(item['date']),
                        'all_users': item['all_users'],
                        'all_registered_users': item['all_registered_users'],
                        'guest_users': item['guest_users'],
                        'normal_users': item['normal_users'],
                        'normal_users_with_refcode': item['normal_users_with_refcode'],
                        'normal_users_without_refcode': item['normal_users_without_refcode'],
                        'normal_users_complete_profile': item['normal_users_complete_profile'],
                        'normal_users_no_complete_profile': item['normal_users_no_complete_profile'],
                        'user_register_with_number': item['user_register_with_number'],
                        'sim_cards_operators': {
                            'MCI-IR': item['sim_cards_operators']['MCI-IR'],
                            'Irancell': item['sim_cards_operators']['Irancell'],
                            'RighTel':item['sim_cards_operators']['RighTel'],
                            'Taliya': item['sim_cards_operators']['Taliya']
                        },
                        'user_register_without_number': item['user_register_without_number'],
                        'phone_register_has_service': item['phone_register_has_service'],
                        'email_register_has_service': item['email_register_has_service'],
                        'phone_register_has_no_service': item['phone_register_has_no_service'],
                        'email_register_has_no_service': item['email_register_has_no_service'],
                        'no_complex_service_with_phone_register': item['no_complex_service_with_phone_register'],
                        'no_complex_service_with_email_register': item['no_complex_service_with_email_register'],
                        'complex_service_with_phone_register': item['complex_service_with_phone_register'],
                        'complex_service_with_email_register': item['complex_service_with_email_register']
                    }
                end_time = datetime.now()
                print('Time Consumed: ', end_time - time_now)
                self.output['data']['item'] = final_report_dashboard
                self.allow_action = False
            except:
                PrintException()
                return False
            self.set_output('public_operations', 'successful')
            return True
