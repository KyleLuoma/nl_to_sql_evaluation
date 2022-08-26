
import pandas as pd
import numpy as np
import db_connector
import get_all_codex_queries_from_sqlite_db as get_codex

df = get_codex.as_dataframe()

df = df.replace({None: ''})
print(df)