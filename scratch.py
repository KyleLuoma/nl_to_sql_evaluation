
import db_connector
import pandas as pd

query = """
select s.query_id, dev.question, gold_query, codex_query, s.result_reason, db_id from codex_queries
join semantic_evaluation se on codex_queries.query_id = se.query_id
join syntactic_evaluation s on codex_queries.query_id = s.query_id
join dev on dev.query_id = se.query_id
left join gold_repair on s.query_id = gold_repair.query_id
where s.is_match = 'UNDETERMINED';
"""


db = db_connector.sqlite_connector('./codex_queries.sqlite')

df = db.do_query(query)
df['man_match'] = ''
df.to_excel('./output/syntactic_undetermined_manual_match.xlsx')