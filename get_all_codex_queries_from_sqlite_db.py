import sqlite3
import pandas as pd



def to_file(filename):
    con = sqlite3.connect('codex_queries.sqlite')
    cur = con.cursor()
    res = cur.execute('select * from codex_queries')
    rows = res.fetchall()
    pd.DataFrame(rows).to_excel(filename)

def as_dataframe():
    con = sqlite3.connect('codex_queries.sqlite')
    cur = con.cursor()
    res = cur.execute("""
        select 
            q.query_id as query_id, 
            d.db_id as db_id,
            d.question as question,
            q.prompt as prompt,
            q.gold_query as gold_query,
            q.codex_query as codex_query
        from codex_queries q
        natural join dev d
        """)
    rows = res.fetchall()
    return pd.DataFrame(rows, columns = [
        'query_id',
        'db_id',
        'question',
        'prompt',
        'gold_query',
        'codex_query'
    ])