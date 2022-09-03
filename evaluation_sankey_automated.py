
import db_connector
import get_all_codex_queries_from_sqlite_db as get_codex
import pandas as pd
import plotly.graph_objects as go

class sk_node:

    def __init__(self, order, df, root = True, label = None, df_index = []):
        if root:
            self.label = order[0]
            self.df_index = []
        else:
            self.label = label
            self.df_index = df_index
        self.targets = []
        self.df = df
        self.value = self.get_value()

        if len(order) > 1:
            self.build_tree(order[1:])

    def get_value(self):
        poss = ['FALSE', 'TRUE', 'UNDETERMINED']
        value = 0
        if len(self.df_index) == 0:
            return self.df['value'].sum()

        elif len(self.df_index) == 1:
            try: 
                value = self.df.loc[self.df_index[0]]['value'].sum()
            except:
                value = 0

        elif len(self.df_index) == 2:
            try: 
                value = self.df.loc[self.df_index[0], self.df_index[1]]['value'].sum()
            except:
                value = 0

        elif len(self.df_index) == 3:
            try: 
                value = self.df.loc[self.df_index[0], self.df_index[1], self.df_index[2]]['value'].sum()
            except:
                value = 0
        
        return value

    def build_tree(self, remaining_order):
        # print(remaining_order)
        if len(remaining_order) > 1 and remaining_order[0] in self.df.reset_index().columns:
            possibilities = self.df.reset_index()[remaining_order[0]].unique()
        else:
            possibilities = ['FALSE', 'TRUE']
        targets = []
        for possibility in possibilities:
            new_target_label = remaining_order[0] + ' - ' + possibility
            new_target_index = self.df_index + [possibility]
            new_target = sk_node(
                remaining_order[0:], self.df, root = False, label = new_target_label, df_index = new_target_index
                )
            targets.append(new_target)
        self.targets = targets

    def to_string(self, indent = ' '):
        node_string = indent + "LABEL: " + self.label + ' INDEX: ' + str(self.df_index) + ' VALUE: ' + str(self.value) + '\n'
        indent += ' '
        for target in self.targets:
            node_string += target.to_string(indent)
        return node_string   

    def get_source_target_value_list(self):
        sources = [self.label for i in range(0, len(self.targets))]
        target_labels = [t.label for t in self.targets]
        values = [t.get_value() for t in self.targets]

        for t in self.targets:
            t_sources, t_target_labels, t_values = t.get_source_target_value_list()
            sources += t_sources
            target_labels += t_target_labels
            values += t_values

        return sources, target_labels, values

db_con = db_connector.sqlite_connector('./codex_queries.sqlite')

# Query to get a pivot of semantic -> db2 -> manual syntax matching with counts
sem_man_db2_q = """
select semantic_comparison_match, db2.is_match as db2_match, manual.is_match as manual_match, count(db2.query_id) as value
from (select se.query_id, se.is_match, se.result_reason
      from syntactic_evaluation se
      where se.query_id not in (select query_id from syntactic_evaluation_manual)
      union
      select sm.query_id, sm.is_match, sm.result_reason
      from syntactic_evaluation_manual sm) as manual
join syntactic_evaluation db2 on db2.query_id = manual.query_id
join semantic_evaluation s on db2.query_id = s.query_id
group by semantic_comparison_match, db2.is_match, manual.is_match
;
"""
res_df = db_con.do_query(sem_man_db2_q)
print(res_df)

# Generate order of sankey categories based on order of columns in df:
order = list(res_df.columns[ : res_df.shape[1] - 1])

labels = [
    'questions'
]

# Generate labels based on order list and result combinations in df:
for cat in order:
    cat_results = res_df[cat].unique()
    for result in cat_results:
        labels.append(cat + ' - ' + result)
labels = labels + ['correct - TRUE', 'correct - FALSE']
# print(labels)

label_dict = {}
label_ix = 0
for label in labels:
    label_dict[label] = label_ix
    label_ix += 1


full_order = ['questions'] + order + ['correct']

label_tree = sk_node(full_order, res_df.set_index(order))
print(label_tree.to_string())
sources, targets, values = label_tree.get_source_target_value_list()
source_ixs = []
target_ixs = []

for source in sources:
    source_ixs.append(label_dict[source])
for target in targets:
    target_ixs.append(label_dict[target])

print(len(sources))
print(len(targets))
print(len(values))

print(sources)
print(values)

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
    "source" : source_ixs, 
    "target" : target_ixs,
    "value"  : values
    # "color"  : [
    #     to_nomatch, to_match, to_udt,
    #     to_nomatch, nomatch_to_match, to_udt,
    #     match_to_nomatch, to_match, to_udt,
    #     match_to_nomatch, udt_to_match, to_udt,
    #     to_match, to_nomatch
    # ]
}

fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = labels,
    #   color = node_colors
    ),
    # link = semantic_first 
    link = semantic_first
)])

fig.update_layout(title_text="CODEX Generated Spider Query Evaluation", font_size=10)
fig.show()

