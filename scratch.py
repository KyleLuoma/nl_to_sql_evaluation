
import cosette_caller

program = """
schema schema_Other_Available_Features(??);
table Other_Available_Features(schema_Other_Available_Features);

query q1 `SELECT count(o.*) FROM Other_Available_Features o`;

query q2 `SELECT  COUNT(o.*) FROM Other_Available_Features o`;

verify q1 q2;
"""

cosette_caller.call_cosette(program)