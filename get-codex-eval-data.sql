select
    cq.query_id,
    d.db_id as database,
    d.question,
    cq.gold_query,
    gr.gold_query_repaired,
    cq.codex_query,
    tr.gold_translated as gold_db2_translation,
    tr.codex_translated as codex_db2_translation,
    syn.is_match as syntactic_comparison_match,
    syn.comparison_type as syntactic_comparison_type,
    syn.result_reason as syntactic_result_reason,
    syn_m.is_match as syntactic_manual_comparison_match,
    syn_m.result_reason as syntactic_manual_result_reason,
    sem.semantic_comparison_match,
    sem.semantic_result_reason,
    sem.codex_query_error_msg,
    sem.codex_results_string

from codex_queries cq
join dev d on cq.query_id = d.query_id
left join semantic_evaluation sem on cq.query_id = sem.query_id
left join syntactic_evaluation syn on cq.query_id = syn.query_id
left join syntactic_evaluation_manual syn_m on cq.query_id = syn_m.query_id
left join gold_repair gr on cq.query_id = gr.query_id
left join translated tr on tr.query_id = cq.query_id