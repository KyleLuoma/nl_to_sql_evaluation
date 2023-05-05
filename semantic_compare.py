import db_connector
import get_all_codex_queries_from_sqlite_db as get_codex
import pandas as pd

codex_df = get_codex.as_dataframe()
spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"

codex_db_con = db_connector.sqlite_connector('./codex_queries.sqlite')

table_name = 'semantic_evaluation'
full_compare = True

try:
    codex_db_con.do_query("drop table {}".format(table_name))
except:
    pass



codex_db_con.do_query("""
create table if not exists {table_name}(
    query_id INT,
    semantic_comparison_match VARCHAR(64),
    semantic_result_reason VARCHAR(256),
    codex_query_error_msg CLOB,
    codex_results_string CLOB,
    gold_results_string CLOB,
    primary key(query_id)
);
""".format(table_name = table_name))

def record_evaluation(
    table_name,
    codex_db_con, 
    query_id, 
    semantic_comparison_match, 
    semantic_result_reason, 
    codex_query_error_msg = "",
    codex_results_string = "", 
    gold_results_string = ""
    ):
    codex_db_con.do_query("""
    insert into {}(
        query_id, semantic_comparison_match, semantic_result_reason, codex_query_error_msg,
        codex_results_string, gold_results_string
    ) values ({}, '{}', '{}', '{}', '{}', '{}')
    """.format(
        table_name, str(query_id), semantic_comparison_match, semantic_result_reason, 
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
            table_name, codex_db_con, row.query_id, 'FALSE', 'query error', err_msg.replace("'", "''")
        )
        continue

    # ------ Analyze semantic correctness (i.e. same results) ------

    # Check if both results are the same size (if not, then not semantically equivalent)
    if codex_results.shape[0] != gold_results.shape[0]:
        record_evaluation(
            table_name, codex_db_con, row.query_id, 'FALSE', 'asymmetrical tuple result size'
        )
        continue

    # Check if codex columns are fewer than gold.
    # If codex is higher than gold, it's still possible that the question was answered.
    if codex_results.shape[1] < gold_results.shape[1]:
        record_evaluation(
            table_name, codex_db_con, row.query_id, 'FALSE', 'Insufficient number of columns in codex result set'
        )
        continue

    # Check if result is an empty set (if empty, then tag as undetermined)
    if codex_results.shape[0] == 0 and gold_results.shape[0] == 0:
        record_evaluation(
            table_name, codex_db_con, row.query_id, 'UNDETERMINED', 'empty result set'
        )
        continue

    # Compare every row of codex results to every row of gold results:
    # If any row of codex results does not match any row of gold results, then not semantically equivalent
    # If we get through all rows of codex results and they all match, then semantically equivalent
    if full_compare:

        codex_gold_col_pairs = [] # List of tuples of (codex_col_name, gold_col_name) that match

        codex_sort_by_col = codex_results.columns[0]
        gold_sort_by_col = gold_results.columns[0]

        max_values = 0

        try:
            # Pair up matching columns in gold and codex results. This allows us to 
            # handle cases where the columns are in different orders, and may also
            # have different names. 
            # It's not sufficient for full evaluation because we're only sorting
            # Individual columns; but we pair up the columns here
            # and do a full dataframe comparison next.
            for codex_col_name in codex_results.columns:
                for gold_col_name in gold_results.columns:

                    codex_col_temp = codex_results[codex_col_name].copy().astype(str)
                    gold_col_temp = gold_results[gold_col_name].copy().astype(str)
                    
                    codex_col_temp.sort_values(inplace = True, ignore_index = True)
                    gold_col_temp.sort_values(inplace = True, ignore_index = True)

                    print(gold_col_temp)
                    print(codex_col_temp)

                    if codex_col_temp.equals(gold_col_temp):
                        codex_gold_col_pairs.append((codex_col_name, gold_col_name))
                        print(codex_col_name, "Value counts:", codex_results[codex_col_name].value_counts().shape[0])
                        if codex_results[codex_col_name].value_counts().shape[0] > max_values:
                            max_values = codex_results[codex_col_name].value_counts().shape[0]
                            codex_sort_by_col = codex_col_name
                            gold_sort_by_col = gold_col_name

        except TypeError as e:
            print("TypeError", e)
            record_evaluation(
                table_name, codex_db_con, row.query_id, 'FALSE', 'type error in column comparison'
            )
            continue

        print("Codex sorting column:", codex_sort_by_col)
        print("Gold sorting column:", gold_sort_by_col)


        print("Performing full comparison")
        print("Sorting codex by", codex_sort_by_col)
        print("Sorting gold by", gold_sort_by_col)
        match = True
        codex_results[codex_sort_by_col] = codex_results[codex_sort_by_col].astype(str)
        gold_results[gold_sort_by_col] = gold_results[gold_sort_by_col].astype(str)
        try:
            codex_results = codex_results.sort_values(by = [codex_sort_by_col])
            gold_results = gold_results.sort_values(by = [gold_sort_by_col])
        except:
            print("sorting failed")

        print("CODEX:")
        print(codex_results)
        print("GOLD:")
        print(gold_results)

        col_matches = 0

        # Using the sorted dataframes, sorted by the most unique columns, we can no
        # compare individual records in each column. If any column does not match,
        # then the result sets are not semantically equivalent.
        for codex_col_ix in range(0, codex_results.shape[1]):
            for gold_col_ix in range(0, gold_results.shape[1]):
                col_matched = True
                for i in range(0, codex_results.shape[0]):
                    if str(codex_results.iloc[i, codex_col_ix]) != str(gold_results.iloc[i, gold_col_ix]):
                        col_matched = False
                        break
                if col_matched:
                    col_matches += 1
                    break
            print("col_matches", col_matches)
                
        if col_matches != gold_results.shape[1]:
            match = False
            notmatched_message = "full tuple compare failed"

        if not match:
            record_evaluation(
                table_name, codex_db_con, row.query_id, 'FALSE', notmatched_message
            )
            pd.concat(
                [codex_results, gold_results], 
                axis = 1, 
                sort = False
                ).to_excel(
                './output/semantic-failure-result-sets/tuple-mismatch-q{}.xlsx'.format(row.query_id)
            )
        else:
            record_evaluation(
                table_name, codex_db_con, row.query_id, 'TRUE', 'full tuple compare succeeded'
            )


    else:
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
            table_name, codex_db_con, row.query_id, 'TRUE', 'could not disprove equivalence'
        )

    


    
    



