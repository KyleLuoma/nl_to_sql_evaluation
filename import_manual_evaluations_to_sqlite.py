
import db_connector
import pandas as pd

query = """
create table syntactic_evaluation_manual
(
    query_id        INT,
    comparison_type VARCHAR(64),
    is_match        VARCHAR(64),
    result_reason   VARCHAR(256),
    primary key (query_id, comparison_type)
);
"""


db = db_connector.sqlite_connector('./codex_queries.sqlite')

db.do_query('drop table syntactic_evaluation_manual')

db.do_query(query)

df = pd.read_excel('./output/semantic_equivalent_syntax_mismatch_manual_eval.xlsx')
df2 = pd.read_excel('./output/syntactic_undetermined_manual_match.xlsx')

insert_query = """
insert into syntactic_evaluation_manual values({}, 'MANUAL', '{}', 'human evaluation')
"""
for row in df.itertuples():
    db.do_query(insert_query.format(
        row.query_id,
        str(row.man_match).upper()
    ))
for row in df2.itertuples():
    db.do_query(insert_query.format(row.query_id, str(row.man_match).upper()))