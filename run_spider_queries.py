import os
import ibm_db
import json

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"
os.chdir(spider_root)

f = open(spider_root + 'dev.json')
dev = json.load(f)
f.close()

for query in dev:
    print(query['question'], query['query'], query['db_id'])