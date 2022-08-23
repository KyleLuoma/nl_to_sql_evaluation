import sqlite3
import pandas as pd
import json

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"

f = open(spider_root + 'dev.json')
dev = json.load(f)
f.close()

con = sqlite3.connect('codex_queries.sqlite')
cur = con.cursor()
# res = cur.execute('select * from codex_queries')
res = cur.execute("""
select * from translated
""")
print(res.fetchall())



