
import db_connector
import pandas as pd

query = """
select s.query_id, dev.question, gold_query, codex_query, db_id from codex_queries
join semantic_evaluation se on codex_queries.query_id = se.query_id
join syntactic_evaluation s on codex_queries.query_id = s.query_id
join dev on dev.query_id = se.query_id
where se.semantic_comparison_match = 'TRUE' and s.is_match = 'FALSE';
"""


db = db_connector.sqlite_connector('./codex_queries.sqlite')

df = db.do_query(query)

df.to_excel('./output/semantic_equivalent_syntax_mismatch.xlsx')
