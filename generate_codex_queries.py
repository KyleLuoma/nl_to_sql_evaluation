import json
import openai
import pandas as pd
import sqlite3
import os
import time


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

query_con = sqlite3.connect('codex_queries.sqlite')
query_cur = query_con.cursor()
query_cur.execute("""create table if not exists codex_queries(
    query_id INT,
    prompt VARCHAR(512),
    gold_query VARCHAR(1024),
    codex_query VARCHAR(1024)
)""")

#Find the last query saved to our local Sqlite database
max_query_id_rs = query_cur.execute("""select max(query_id) as last_saved from codex_queries""")
max_query_id = max_query_id_rs.fetchone()
print(max_query_id[0])
if max_query_id[0] != None:
    query_num = max_query_id[0] + 1

query_nums = []
codex_prompts = []
gold_queries = []
codex_queries = []

while (query_num - 10000) < len(dev):
    query = dev[query_num - 10000]
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
    try:
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
    except openai.error.RateLimitError:
        print("Encountered rate limit error when calling openai API")
        print("Sleeping for 10 seconds then trying the query again.")
        for i in range(0, 10):
            print(str(i + 1), end = " ", flush = True)
            time.sleep(1)
        print('\n', end = '\n')
        continue

    #Sleep to prevent a high api call rate failure
    print("Sleeping for:", end = " ")
    for i in range(0, 4):
        print(str(i + 1), end = " ", flush = True)
        time.sleep(1)
    print('\n', end = '\n')

    codex_query = 'SELECT ' + response.choices[0]['text']
    gold_query = query['query']

    query_nums.append(query_num)
    codex_prompts.append(codex_prompt)
    gold_queries.append(gold_query)
    codex_queries.append(codex_query)

    replace_chars = [
        ("'", "''"),
        ("%", "")
    ]

    for char in replace_chars:
        codex_prompt = codex_prompt.replace(char[0], char[1])
        gold_query = gold_query.replace(char[0], char[1])
        codex_query = codex_query.replace(char[0], char[1])

    print('Gold query:', gold_query)
    print('Codex query:', codex_query)

    insertion_query = """
        insert into codex_queries(query_id, prompt, gold_query, codex_query) 
        values({}, '{}', '{}', '{}')""".format(
            str(query_num),
            str(codex_prompt),
            str(gold_query),
            str(codex_query)
        )

    print(insertion_query)

    max_query_id_rs = query_cur.execute(insertion_query)
    query_con.commit()

    query_num += 1

codex_query_df = pd.DataFrame(data = {
    'query_id': query_nums,
    'prompt': codex_prompts,
    'gold_query': gold_queries,
    'codex_query:': codex_queries
    })
codex_query_df.to_excel('./codex_generated_queries.xlsx')