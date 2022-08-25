import sqlite3
import pandas as pd


def to_file(filename):
    pd.DataFrame(as_dataframe()).to_excel(filename)

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
            t.gold_translated as gold_translated,
            q.codex_query as codex_query,
            t.codex_translated as codex_translated
        from codex_queries q
        natural join dev d
        left join translated t on q.query_id = t.query_id
        """)
    rows = res.fetchall()
    return pd.DataFrame(rows, columns = [
        'query_id',
        'db_id',
        'question',
        'prompt',
        'gold_query',
        'gold_translated',
        'codex_query',
        'codex_translated'
    ])

if __name__ == "__main__":
    to_file("./explained_queries.xlsx")