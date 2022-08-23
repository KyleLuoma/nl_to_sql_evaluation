import pandas as pd
import ibm_db
import get_all_codex_queries_from_sqlite_db as get_queries
import json

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"

f = open('./.local/db2.json')
db2_params = json.load(f)
f.close()
db2_con = ibm_db.connect(db2_params['db2_connect_params'], "", "")

queries = get_queries.as_dataframe()
print(queries)

#DB2 explain syntax:
# explain all set queryno=<INT> for <QUERY>

f = open(spider_root + 'dev.json')
dev = json.load(f)
f.close()


# Gold query explanations will have query id in 1xxxx range
# Codex query explanations will be in 2xxxx range, where xxxx corresponds to the query id
# in the queries dataframe
failures = []
for query in queries.itertuples():
    ibm_db.exec_immediate(db2_con, "set current schema = " + query.db_id)
    explain_gold = """
        explain all set queryno={} for {}
    """.format(
        str(query.query_id),
        query.gold_query
    )
    explain_codex = """
        explain all set queryno={} for {}
    """.format(
        str(query.query_id + 10000),
        query.codex_query
    )
    print(explain_gold)
    try:
        ibm_db.exec_immediate(db2_con, explain_gold)
    except:
        failures.append(query.query_id)

    print(explain_codex)
    try:
        ibm_db.exec_immediate(db2_con, explain_codex)
    except:
        failures.append(query.query_id + 10000)

pd.DataFrame({'failed_query_id': failures}).to_excel('explain_failures.xlsx')
