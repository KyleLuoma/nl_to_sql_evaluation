import json
import openai
import pandas as pd
import sqlite3
import os


spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"

print(os.getcwd())

f = open('.local/openai.json')
openai_key = json.load(f)
f.close()

f = open(spider_root + 'dev.json')
dev = json.load(f)
f.close()

openai.api_key = openai_key['api_key']
openai.Model.list()

table_retrieval = """
select * from sqlite_master where type = 'table';
"""

column_info = """
pragma table_info('{}')
"""
query_num = 10000

query_nums = []
codex_prompts = []
gold_queries = []
codex_queries = []

for query in dev:
    db_path = spider_root + 'database/' + query['db_id'] + '/' + query['db_id'] + '.sqlite'
    con = sqlite3.connect(db_path)
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    cur = con.cursor()
    res = cur.execute(table_retrieval)
    tables_rs = res.fetchall()
    tables_df = pd.DataFrame(tables_rs)
    codex_prompt = "#DB2 SQL tables, with their properties:\n#\n"
    for row in tables_df.itertuples():
        table_name = row[2]
        codex_prompt += ("#" + table_name + "(")

        res = cur.execute(column_info.format(table_name))
        col_df = pd.DataFrame(res.fetchall())
        columns = col_df[1].to_list()
        for column in columns:
            codex_prompt += (column + ',')
        codex_prompt = codex_prompt[ : len(codex_prompt) - 1] #remove trailing comma
        codex_prompt += ')\n'

    codex_prompt += "#\n### a sql query to answer the question: " + query['question'] + '\nSELECT'

    #OPEN AI API CALL:
    print("Calling Codex API for query", str(query_num))
    response = openai.Completion.create(
        model="code-davinci-002",
        prompt=codex_prompt,
        temperature=0,
        max_tokens=150,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0,
        stop=["#", ";"]
        )

    print(response.choices[0]['text'])
    codex_query = 'SELECT ' + response.choices[0]['text']
    gold_query = query['query']

    query_nums.append(query_num)
    codex_prompts.append(codex_prompt)
    gold_queries.append(gold_query)
    codex_queries.append(codex_query)

    query_num += 1

codex_query_df = pd.DataFrame(data = {
    'query_id': query_nums,
    'prompt': codex_prompts,
    'gold_query': gold_queries,
    'codex_query:': codex_queries
    })
codex_query_df.to_excel('./codex_generated_queries.xlsx')