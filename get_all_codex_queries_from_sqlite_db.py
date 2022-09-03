import sqlite3
import pandas as pd


def to_file(filename, include_translated = False, repaired = False):
    pd.DataFrame(as_dataframe(include_translated = include_translated, repaired = repaired)).to_excel(filename)

def as_dataframe(include_translated = False, repaired = False):
    con = sqlite3.connect('codex_queries.sqlite')

    columns = [
        'query_id',
        'db_id',
        'question',
        'prompt',
        'gold_query',
        'codex_query'
    ]

    query = """
        select 
            q.query_id as query_id, 
            d.db_id as db_id,
            d.question as question,
            q.prompt as prompt,
            q.gold_query as gold_query,
            q.codex_query as codex_query{}
        from codex_queries q
        natural join dev d{}
        """
    translated = 'left join translated t on q.query_id = t.query_id'

    if include_translated:
        columns = columns + ['gold_translated', 'codex_translated']
        query = query.format(
            ',\nt.gold_translated as gold_translated,\nt.codex_translated as codex_translated',
            '\n' + translated
            )

    else:
        query = query.format('', '')
        
    cur = con.cursor()
    res = cur.execute(query)
    rows = res.fetchall()

    res_df = pd.DataFrame(rows, columns = columns)
    res_df = res_df.set_index('query_id')

    if repaired:
        repaired_query = """
            select * from gold_repair
        """
        res = cur.execute(repaired_query)
        rep_rows = res.fetchall()
        rep_df = pd.DataFrame(rep_rows, columns = ['query_id', 'gold_query_repaired'])
        rep_df = rep_df.set_index('query_id')    
        print(rep_df)
        for row in rep_df.itertuples():
            res_df.at[row.Index, 'gold_query'] = row.gold_query_repaired

    return res_df.reset_index()

if __name__ == "__main__":
    to_file("./output/explained_queries_repaired.xlsx", repaired = True)