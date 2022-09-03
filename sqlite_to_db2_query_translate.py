
import db_connector
import pandas as pd

query = """
select s.query_id, gold_query, codex_query, db_id from codex_queries
join semantic_evaluation se on codex_queries.query_id = se.query_id
join syntactic_evaluation s on codex_queries.query_id = s.query_id
join dev on dev.query_id = se.query_id
where s.is_match = 'UNDETERMINED' and s.result_reason = 'failed db2 gold translation';
"""


db = db_connector.sqlite_connector('./codex_queries.sqlite')

df = db.do_query(query)

new_table = """
create table gold_repair(
    query_id int,
    gold_query_repaired varchar(1024)
)
"""

try:
    db.do_query('drop table gold_repair')
except:
    pass

db.do_query(new_table)

new_gold_qs = []

for row in df.itertuples():

    new_gold_q = ''

    # Psuedo parser to fix group by issue with spider sqlite queries:

    if 'group by' in row.gold_query.lower():
        gold_q = row.gold_query.lower()
        gb_start = gold_q.find('group by')

        if 'having' in gold_q:
            gb_end = gold_q.find('having')
        elif 'order by' in gold_q:
            gb_end = gold_q.find('order by')
        elif 'limit' in gold_q:
            gb_end = gold_q.find('limit')
        else:
            gb_end = len(gold_q)

        old_gb = gold_q[gb_start : gb_end]
# select t1.countryid ,  t1.countryname from countries as t1 join car_makers as t2 on t1.countryid  =  t2.country group by t1.countryid having count(*)  >  3 union select t1.countryid ,  t1.countryname from countries as t1 join car_makers as t2 on t1.countryid  =  t2.country join model_list as t3 on t2.id  =  t3.maker where t3.model  =  'fiat';
        toks = gold_q.split(' ')
        new_gb = 'group by '
        done = False
        kw_list = ['count(', 'avg(', 'sum(', 'max(', 'min(', 'from']
        for tok in toks[1 : ]:
            for kw in kw_list:
                if kw in tok:
                    done = True
            if not done:
                new_gb += (tok + ' ')
            else:
                break
        new_gb = new_gb.strip()
        if new_gb[len(new_gb) - 1] == ',':
            new_gb = new_gb[:len(new_gb) - 1]
        new_gold_q = gold_q.replace(old_gb, new_gb + ' ')

    if '"' in row.gold_query:
        new_gold_q = row.gold_query.lower().replace('"', "'")

    print (new_gold_q)

    new_gold_qs.append(new_gold_q)
    db.do_query(
        "insert into gold_repair values ({}, '{}')".format(
            row.query_id, 
            new_gold_q.replace("'", "''").strip()
        )
        )

df['new_gold'] = new_gold_qs

df.to_excel('./output/semantic_equivalent_syntax_undefined.xlsx')
