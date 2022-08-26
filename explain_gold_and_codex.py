# Execute spider gold queries and codex generated queries using explain in 
# our DB2 instrance.
# Note query id scheme below.
# WARNING: Do not run multiple times, as this will generate multiple duplicate
# rows in the db2 explain table.

# ---- Boolean variable to enable / disable db2 explain action in script:
do_explain = False


import pandas as pd
import ibm_db
import get_all_codex_queries_from_sqlite_db as get_queries
import json
import sqlite3

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"

f = open('./.local/db2.json')
db2_params = json.load(f)
f.close()
db2_con = ibm_db.connect(db2_params['db2_connect_params'], "", "")

queries = get_queries.as_dataframe()

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
    if not do_explain:
        break
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
    if do_explain:
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
            
# pd.DataFrame({'failed_query_id': failures}).to_excel('explain_failures.xlsx')

# Retrieve query explanations from db2 and store them in our
# sqlite database:
explain_statement_query = """
select
    case
        when es_gold.query_id IS NULL then es_codex.query_id
        when es_codex.query_id IS NULL then es_gold.query_id
        else es_gold.query_id
    end as query_id,
    es_gold.statement_text as gold_translated,
    es_codex.statement_text as codex_translated
from (
    select
        queryno as query_id,
        statement_text
    from systools.explain_statement
    where
        explain_level = 'P'
        and queryno >= 10000
        and queryno < 20000
) as es_gold
full outer join(
    select
        queryno - 10000 as query_id,
        statement_text
    from systools.explain_statement
    where
        explain_level = 'P'
        and queryno >= 20000
) as es_codex
on es_codex.query_id = es_gold.query_id
order by query_id
"""
stmt = ibm_db.exec_immediate(db2_con, explain_statement_query)
dictionary = ibm_db.fetch_assoc(stmt)

query_con = sqlite3.connect('codex_queries.sqlite')
query_cur = query_con.cursor()

try:
    query_cur.execute("drop table translated")
except:
    pass
query_cur.execute("""
    create table translated(
        query_id INT PRIMARY KEY ,
        gold_translated CLOB,
        codex_translated CLOB
    )
    """)
query_con.commit()

while dictionary != False:
    print(dictionary)

    gold = dictionary['GOLD_TRANSLATED']
    codex = dictionary['CODEX_TRANSLATED']

    for symbol in [("'", "''"), ("%", "")]:
        if gold != None:
            gold = gold.replace(symbol[0], symbol[1])
        else:
            gold = ''
        if codex != None:
            codex = codex.replace(symbol[0], symbol[1])
        else:
            codex = ''

    query_cur.execute("""
        insert into translated(query_id, gold_translated, codex_translated)
        values({}, '{}', '{}')
    """.format(
        int(dictionary['QUERY_ID']),
        gold,
        codex
    ))
    query_con.commit()
    dictionary = ibm_db.fetch_assoc(stmt)