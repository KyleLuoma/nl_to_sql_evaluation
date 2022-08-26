import db_connector
import get_all_codex_queries_from_sqlite_db as get_codex
import pandas as pd

codex_df = get_codex.as_dataframe()
spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"

codex_db_con = db_connector.sqlite_connector('./codex_queries.sqlite')

try:
    codex_db_con.do_query("drop table semantic_evaluation")
except:
    pass

codex_db_con.do_query("""
create table if not exists semantic_evaluation(
    query_id INT,
    semantic_comparison_match VARCHAR(64),
    semantic_result_reason VARCHAR(256),
    codex_query_error_msg CLOB,
    codex_results_string CLOB,
    gold_results_string CLOB,
    primary key(query_id)
);
""")

def record_evaluation(
    codex_db_con, 
    query_id, 
    semantic_comparison_match, 
    semantic_result_reason, 
    codex_query_error_msg = "",
    codex_results_string = "", 
    gold_results_string = ""
    ):
    codex_db_con.do_query("""
    insert into semantic_evaluation(
        query_id, semantic_comparison_match, semantic_result_reason, codex_query_error_msg,
        codex_results_string, gold_results_string
    ) values ({}, '{}', '{}', '{}', '{}', '{}')
    """.format(
        str(query_id), semantic_comparison_match, semantic_result_reason, 
        codex_query_error_msg, codex_results_string, gold_results_string
    ))


for row in codex_df.itertuples():
    gold = row.gold_query
    codex = row.codex_query
    db = spider_root + "database/" + row.db_id + "/" + row.db_id + ".sqlite"
    db_con = db_connector.sqlite_connector(db)

    # Fetch results from sqlite spider databases:

    print("\n\n\n----------------------------------------------------------------")
    print("Query ID", row.query_id)
    print("Question:", row.question)

    try:
        print("\nRunning GOLD", row.gold_query.strip())
        gold_results = db_con.do_query(gold)
        print("\n", gold_results)
    except:
        pass

    try:
        print("\nRunning CODEX", row.codex_query.strip())
        codex_results = db_con.do_query(codex)
        print("\n", codex_results)
    except db_connector.sqlite3.OperationalError as op_error:
        err_msg = op_error.args[0]
        codex_results = pd.DataFrame()
        record_evaluation(
            codex_db_con, row.query_id, 'FALSE', 'query error', err_msg.replace("'", "''")
        )
        continue

    # ------ Analyze semantic correctness (i.e. same results) ------

    # Check if both results are the same size (if not, then not semantically equivalent)
    if codex_results.shape[0] != gold_results.shape[0]:
        record_evaluation(
            codex_db_con, row.query_id, 'FALSE', 'asymmetrical tuple result size'
        )
        continue

    if codex_results.shape[1] != gold_results.shape[1]:
        record_evaluation(
            codex_db_con, row.query_id, 'FALSE', 'asymmetrical column result size'
        )
        continue

    # Check if result is an empty set (if empty, then tag as undetermined)
    if codex_results.shape[0] == 0 and gold_results.shape[0] == 0:
        record_evaluation(
            codex_db_con, row.query_id, 'UNDETERMINED', 'empty result set'
        )
        continue

    # For the first tuple in gold results, gold_results.iloc[0]
    #    For each tuple in codex results compare to gold results.
    #    We need to perform a bag comparison of two bags with items in an arbitrary order

    # Disprove that for every tuple in gold there exists at least one matching tuple in codex:
    # Do this by cycling through all tuples in codex to see if we can find a situation where no
    # tuple in codex matches the first tuple in the gold result set.
    # If the results are equivalent, then there must be at least one tuple in codex that matches
    # any given tuple (including the first one) in gold.
    codex_matched_gold_row = False
    gold_list = [str(i) for i in gold_results.iloc[0].values]
    gold_list.sort()
    for codex_row in codex_results.iterrows():
        # Create sorted lists of codex and gold tuples:
        codex_list = [str(i) for i in codex_row[1].values]
        codex_list.sort()
        if codex_list == gold_list:
            codex_matched_gold_row = True
#    If there are no matches, then mark as not semantically equivalent
    if not codex_matched_gold_row:
        print(row.query_id)
        record_evaluation(
            codex_db_con, row.query_id, 'FALSE', 'codex tuples not matched to gold tuple'
        )
        continue

    # Now do the opposite, search all gold tuples for a match against first codex tuple
    gold_matched_codex_row = False
    codex_list = [str(i) for i in codex_results.iloc[0].values]
    codex_list.sort()
    for gold_row in gold_results.iterrows():
        gold_list = [str(i) for i in gold_row[1].values]
        gold_list.sort()
        if gold_list == codex_list:
            gold_matched_codex_row = True
    if not gold_matched_codex_row:
        print(row.query_id)
        record_evaluation(
            codex_db_con, row.query_id, 'FALSE', 'gold tuples not matched to codex tuple'
        )
        continue

    # If we get this far, mark as semantically equivalent.
    record_evaluation(
        codex_db_con, row.query_id, 'TRUE', 'could not disprove equivalence'
    )

    


    
    



