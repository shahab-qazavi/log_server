from datetime import datetime

from publics import PrintException, db

first_time = datetime.now()

col_logs = db()['logs']
module_id = ""
try:
    for items in col_logs.find({}, {'url': 1}):
        object_id = items['_id']
        full_url = items['url']
        url = ""
        c = 0
        for item in full_url:
            if item != "?":
                url += item
                if item == "/":
                    c += 1
            else:
                break

        module_id = ""
        module = ""
        ids = ""

        if c != 1:
            for item in url[::-1]:
                if item != "/":
                    try:
                        int(item)
                        ids = 'yes'
                    except:
                        pass
                    module += item
                else:
                    break
            module = module[::-1]
        elif c == 1:
            pass

        if ids == 'yes':
            module_id = module
            url = url.rstrip(module)
            url += "id"
            q_url = url.rstrip("/id")
            module = ""
            for i in q_url[::-1]:
                if i != "/":
                    module += i
                else:
                    break
        elif ids == "":
            module = ""
            for item in url[::-1]:
                if item == "/":
                    break
                else:
                    module += item
        url_check = url.lstrip("/")
        url_check = url_check.rstrip("/")
        if module == "" and c == 2 and (url_check == "v2" or url_check == "v1"):
            url += "home"
            module = "home"
        elif module == "" and c == 2:
            module = url_check
        else:
            module = module[::-1]

        col_logs.update_one({"_id": object_id},
                            {"$set": {"module": module,'module_id':module_id, "full_url": full_url,
                                      "url": url, "set": True}})
except:
    PrintException()


print(datetime.now() - first_time)
