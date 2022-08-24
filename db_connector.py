import os
import json
import sqlite3
import ibm_db
import psycopg2
import pandas as pd
from collections import defaultdict

class db_connector:
    def __init__(self):
        pass
    def do_query(self, query):
        pass
    def close_connection(self):
        pass
    
class db2_connector(db_connector):

    def __init__(self):
        f = open('./.local/db2.json')
        db2_params = json.load(f)
        f.close()
        self.con = ibm_db.connect(db2_params['db2_connect_params'], "", "")

    def do_query(self, query):

        def default_value():
            return []

        stmt = ibm_db.exec_immediate(self.con, query)
        dict = ibm_db.fetch_assoc(stmt)
        results = defaultdict(default_value)
        while dict != False:
            for column in dict.keys():
                results[column].append(dict[column])
        result_df = pd.DataFrame(results)
        return result_df

    def close_connection(self):
        ibm_db.close(self.con)



class postgresql_connector(db_connector):

    def __init__(self):
        f = open('./.local/postgresql.json')
        postgresql_params = json.load(f)
        f.close()
        self.con = psycopg2.connect(
            host = "localhost",
            port = 5432,
            user = postgresql_params['username'],
            password = postgresql_params['password']
            )
        self.cur = self.con.cursor()

    def do_query(self, query):

        def default_value():
            return []
        self.con.commit()
        self.cur.execute(query)
        if self.cur.description == None:
            self.con.commit()
            return pd.DataFrame()
        columns = [d[0] for d in self.cur.description]
        results = self.cur.fetchall()
        result_dict = defaultdict(default_value)
        for i in range(0, len(columns)):
            for tuple in results:
                result_dict[columns[i]].append(tuple[i])
        result_df = pd.DataFrame(result_dict)
        self.con.commit()
        return result_df

    def close_connection(self):
        self.cur.close()
        self.con.close()
