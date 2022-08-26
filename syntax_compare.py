import db_connector
import get_all_codex_queries_from_sqlite_db as get_codex
import pandas as pd

codex_df = get_codex.as_dataframe()
spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"
codex_db_con = db_connector.sqlite_connector('./codex_queries.sqlite')

try:
    codex_db_con.do_query("drop table syntactic_evaluation")
except:
    pass

codex_db_con.do_query("""
    create table if not exists syntactic_evaluation(
    query_id INT,
    comparison_type VARCHAR(64),
    is_match VARCHAR(64),
    result_reason VARCHAR(256),
    primary key(query_id, comparison_type)
);
""")

def record_evaluation(
    db_con,
    query_id,
    comparison_type,
    is_match,
    result_reason
    ):
    db_con.do_query("""
    insert into syntactic_evaluation(query_id, comparison_type, is_match, result_reason)
    values ({}, '{}', '{}', '{}')
    """.format(str(query_id), comparison_type, is_match, result_reason))

codex_df = codex_df.replace({None : ''})

for row in codex_df.itertuples():
    print(len(row.gold_translated), len(row.codex_translated))
    if row.gold_translated == row.codex_translated:
        record_evaluation(
            codex_db_con, row.query_id, 'db2_translate', 'TRUE', 'translated query match'
        )
    elif len(row.gold_translated) == 0 and len(row.codex_translated) > 0:
        record_evaluation(
            codex_db_con, row.query_id, 'db2_translate', 'UNDETERMINED', 'failed db2 gold translation'
        )
    elif len(row.codex_translated) == 0 and len(row.gold_translated) > 0:
        record_evaluation(
            codex_db_con, row.query_id, 'db2_translate', 'UNDETERMINED', 'failed db2 codex translation'
        )
    elif len(row.codex_translated) == 0 and len(row.gold_translated) == 0:
        record_evaluation(
            codex_db_con, row.query_id, 'db2_translate', 'UNDETERMINED', 'failed db2 codex and gold translations'
        )
    elif (len(row.gold_translated) > 0 and len(row.codex_translated) > 0) and row.gold_translated != row.codex_translated:
        record_evaluation(
            codex_db_con, row.query_id, 'db2_translate', 'FALSE', 'translated queries do not match'
        )
