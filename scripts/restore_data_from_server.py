import os
os.chdir('/root')
os.system("tar xvfz data_for_log.tar.gz")

collections = ['search_history', 'events', 'chat', 'users', 'services']
for collection in collections:
    os.system("mongoimport --db %s --collection %s --file %s.json" % ('ldb', collection, collection))
    os.system("rm %s.json" % collection)

