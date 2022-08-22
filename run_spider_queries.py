import os
import ibm_db
import json

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"

f = open(spider_root + 'dev.json')
dev = json.load(f)
f.close()

f = open('./.local/db2.json')
db2_params = json.load(f)
f.close()
db2_con = ibm_db.connect(db2_params['db2_connect_params'], "", "")

for query in dev:
    print(query['question'], query['query'], query['db_id'])
    ibm_db.exec_immediate(db2_con, "set current schema = " + query['db_id'])
    stmt = ibm_db.exec_immediate(db2_con, query['query'])
    dictionary = ibm_db.fetch_assoc(stmt)
    while dictionary != False:
        print(dictionary)
        dictionary = ibm_db.fetch_assoc(stmt)