
import pandas as pd
import db_connector

db = db_connector.sqlite_connector("//192.168.1.17/data/nl_benchmarks/spider/database/aircraft/aircraft.sqlite")

resdf = db.do_query("select * from pilot")
print(resdf.iloc[0].values)

for val in resdf.iloc[0]:
    print(val)


print([1, 2, 3] == [1, 3, 2])
