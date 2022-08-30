
import db_connector
import get_all_codex_queries_from_sqlite_db as get_codex
import pandas as pd
import plotly.graph_objects as go

db_con = db_connector.sqlite_connector('./codex_queries.sqlite')

sem_res_df = db_con.do_query("""
select semantic_comparison_match, count(query_id) 
from semantic_evaluation 
group by semantic_comparison_match
""")
sem_res_df.set_index(['semantic_comparison_match'], inplace = True)

syn_res_df = db_con.do_query("""
select is_match, count(distinct query_id)
from syntactic_evaluation
group by is_match
""")
syn_res_df.set_index(['is_match'], inplace = True)

res_df = db_con.do_query("""
select semantic_comparison_match, is_match, count(distinct sem.query_id) as qry_count
from semantic_evaluation sem
join syntactic_evaluation syn on sem.query_id = syn.query_id
group by semantic_comparison_match, is_match;
""")
print(res_df)
res_df.set_index(['semantic_comparison_match', 'is_match'], inplace = True)
print(res_df.loc['FALSE', 'FALSE'][0])

labels = [
    "Questions",
    "Semantic - No Match",
    "Semantic - Match",
    "Semantic - Undetermined",
    "Syntactic - No Match",
    "Syntactic - Match",
    "Syntactic - Undetermined",
]

match_color =           "rgba(1, 54, 15, 0.8)"
no_match_color =        "rgba(156, 100, 17, 0.8)"
undetermined_color =    "rgba(62, 73, 92, 0.8)"
to_udt =                "rgba(106, 122, 111, 0.8)"
to_nomatch =            "rgba(161, 118, 63, 0.8)"
to_match =              "rgba(109, 122, 75, 0.8)"
match_to_nomatch =      "rgba(153, 116, 6, 0.8)"
nomatch_to_match =      "rgba(202, 219, 72, 0.8)"
udt_to_match =          "rgba(62, 92, 78, 0.8)"

link_colors = [to_nomatch, to_match, to_udt]

node_colors = [
    undetermined_color,             # "Questions"
    no_match_color,                 # "Semantic - No Match"
    match_color,                    # "Semantic - Match"          
    undetermined_color,             # "Semantic - Undetermined"
    no_match_color,                 # "Syntactic - No Match"
    match_color,                    # "Syntactic - Match"
    undetermined_color,             # "Syntactic - Undetermined"
]

#Display semantic matches first, then syntactic
semantic_first = {
    "source" : [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3], 
    "target" : [1, 2, 3, 4, 5, 6, 4, 5, 6, 4, 5, 6],
    "value"  : [
        sem_res_df.loc['FALSE'][0], sem_res_df.loc['TRUE'][0], sem_res_df.loc['UNDETERMINED'][0], 
        res_df.loc['FALSE', 'FALSE'], res_df.loc['FALSE', 'TRUE'], res_df.loc['FALSE', 'UNDETERMINED'],
        res_df.loc['TRUE', 'FALSE'], res_df.loc['TRUE', 'TRUE'], res_df.loc['TRUE', 'UNDETERMINED'],
        res_df.loc['UNDETERMINED', 'FALSE'], res_df.loc['UNDETERMINED', 'TRUE'], res_df.loc['UNDETERMINED', 'UNDETERMINED']
        ],
    "color"  : [
        to_nomatch, to_match, to_udt,
        to_nomatch, nomatch_to_match, to_udt,
        match_to_nomatch, to_match, to_udt,
        match_to_nomatch, udt_to_match, to_udt
    ]
}

syn_order = [
        syn_res_df.loc['FALSE'][0], syn_res_df.loc['TRUE'][0], syn_res_df.loc['UNDETERMINED'][0],
        res_df.loc['FALSE', 'FALSE'], res_df.loc['TRUE', 'FALSE'], res_df.loc['UNDETERMINED', 'FALSE'],
        res_df.loc['FALSE', 'TRUE'], res_df.loc['TRUE', 'TRUE'], res_df.loc['UNDETERMINED', 'TRUE'],
        res_df.loc['FALSE', 'UNDETERMINED'], res_df.loc['TRUE', 'UNDETERMINED'], res_df.loc['UNDETERMINED', 'UNDETERMINED']
    ]

syntactic_first = {
    "source" : [0, 0, 0, 4, 4, 4, 5, 5, 5, 6, 6, 6],
    "target" : [4, 5, 6, 1, 2, 3, 1, 2, 3, 1, 2, 3],
    "value"  : syn_order,
    "color"  : [
        to_nomatch, to_match, to_udt,
        to_nomatch, nomatch_to_match, to_udt,
        match_to_nomatch, to_match, to_udt,
        match_to_nomatch, udt_to_match, to_udt
    ]
}

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = labels,
      color = node_colors
    ),
    # link = semantic_first 
    link = semantic_first
)])

fig.update_layout(title_text="CODEX Generated Spider Query Evaluation", font_size=10)
fig.show()

