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
cur.execute('drop table dev')
res = cur.execute("""
create table dev(
    query_id INT,
    db_id VARCHAR(64),
    query VARCHAR(1024),
    question VARCHAR(1024)
)
""")
con.commit()
query_id = 10000
for query in dev:

    insert_string = """
    insert into dev(query_id, db_id, question) values(
        {}, '{}', '{}'
    )
    """.format(
        str(query_id),
        str(query['db_id']),
        str(query['question'].replace("'", "''"))
    )
    print(insert_string)
    res = cur.execute(insert_string)
    con.commit()

    query_id += 1


