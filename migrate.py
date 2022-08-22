import os
import json
import sqlite3
import ibm_db
import pandas as pd

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/database/"
# os.chdir(spider_root)

def build_table_creation_sql(table_name, col_df):
    primary_keys = col_df.where(col_df[5] >= 1).dropna(how = "all")
    sql = "CREATE TABLE IF NOT EXISTS " + table_name + "(\n"
    for col in col_df.itertuples():
        column_name = col[2]
        if (
            "%" in column_name 
            or "(" in column_name 
            or ")" in column_name 
            or " " in column_name
            or column_name[0].isnumeric()
            ):
            column_name = '"' + column_name + '"'
        sql = sql + column_name + " "
        data_type = col[3].upper()
        if "INT" in data_type:
            data_type = "INTEGER"
        if ("TEXT" in data_type or "CHAR" in data_type) and col[6] >= 1: #text and is pk
            data_type = "VARCHAR(64)"
        if ("TEXT" in data_type or "CHAR" in data_type) and col[6] == 0: #text and not pk
            data_type = "VARCHAR(512)"
        if "BIT" in data_type:
            data_type = "SMALLINT"
        if "BOOL" in data_type:
            data_type = "BOOLEAN"
        if "NUMBER" in data_type:
            data_type = "DOUBLE"
        if "FLOAT" in data_type:
            data_type = "DOUBLE"
        if "DATE" in data_type:
            data_type = "VARCHAR(127)"
        if "YEAR" in data_type:
            data_type = "INTEGER"
        if len(data_type) == 0:
            data_type = "VARCHAR(256)"
        
        sql = sql + data_type + " "                        
        if col[6] >= 1 or col[4] >= 1:                                 
            sql = sql + "NOT NULL , \n"
        else:
            sql = sql + ', \n'
    if primary_keys.shape[0] > 0:
        sql = sql + 'primary key('
        for row in primary_keys.itertuples():
            sql = sql + row[2] + ' , '
        sql = sql + "_BBB"
        sql = sql.replace(", _BBB", ")")
    else:
        sql = sql + '_AAA'
        sql = sql.replace(', \n_AAA', '')
    sql = sql + ")"
    print(sql)
    print(col_df)
    if(table_name == "accelerator_compatible_browser"):
        pass
    return sql

def build_insert_sql(table_name, values_df, col_rs_df):
    statements = []
    for row in values_df.itertuples():
        sql = "INSERT INTO {} VALUES (".format(table_name)
        colnum = 0
        # print(col_rs_df)
        for val in row[1:]:
            if val == None:
                val = 'none'
            dtype = col_rs_df[2][colnum]
            is_text = (
                'text' in dtype.lower() 
                or 'char' in dtype.lower()
                or 'date' in dtype.lower()
                or 'time' in dtype.lower()
                or len(dtype) == 0
            )
            if is_text:
                val = str(val).replace("'", "''")
                sql = sql + "'" + str(val) + "' , "
            elif len(str(val)) > 0 and str(val).isnumeric():
                sql = sql + str(val) + " , "
            else:
                sql = sql + str(-1) + " , "

            colnum += 1
        sql = sql + "_AAA"
        sql = sql.replace(", _AAA", "")
        sql = sql + ")"
        statements.append(sql)
    return statements

table_retrieval = """
select * from sqlite_schema where type = 'table' order by name;
"""

column_info = """
pragma table_info('{}')
"""
f = open('./.local/db2.json')
db2_params = json.load(f)
f.close()
db2_con = ibm_db.connect(db2_params['db2_connect_params'], "", "")
# ibm_db.exec_immediate(db2_con, "CREATE TABLE IF NOT EXISTS Activity (actid INTEGER PRIMARY KEY NOT NULL, activity_name varchar(25))")

# Use to skip already completed dbs:
start_with_db = "academic"
do_create = False

# Log all insertions that get skipped during etl:
skip_log = open("./skiplog.txt", 'a')
skip_log.write("schema_name | table_name | statement \n")

#Iterate through each sqlite db in the spider dataset
for subdir, dirs, files in os.walk(spider_root):
    for file in files:
        sqlite_db_path = ""
        schema_name = ""
        if(".sqlite" in file):
            sqlite_db_path = subdir + "/" + file
            schema_name = file.replace(".sqlite", "")
            if schema_name == start_with_db:
                do_create = True
            if not do_create:
                continue
            # Create schema in db2
            try:
                ibm_db.exec_immediate(db2_con, "create schema " + schema_name)
            except:
                print("Schema", schema_name, "already exists")

            ibm_db.exec_immediate(db2_con, "set current schema = " + schema_name)

        #Create sqlite connection
        con = sqlite3.connect(sqlite_db_path)
        con.text_factory = lambda b: b.decode(errors = 'ignore')
        cur = con.cursor()
        res = cur.execute(table_retrieval)
        table_rs = res.fetchall()
        table_rs_df = pd.DataFrame(table_rs)


        # Create tables in DB2 instance using sqlite schema data:
        for row in table_rs_df.itertuples():
            table_name = row[2]
            print(" ---- SCHEMA ", schema_name, " TABLE ", table_name, " ---- ")
            res = cur.execute(column_info.format(table_name))
            col_rs = res.fetchall()
            col_rs_df = pd.DataFrame(col_rs)
            # print(col_rs_df)
            table_creation_sql = build_table_creation_sql(table_name, col_rs_df)
            # print(table_creation_sql)

            if table_name != 'sqlite_sequence':
                try:
                    ibm_db.exec_immediate(db2_con, "drop table " + table_name)
                except:
                    pass
                ibm_db.exec_immediate(db2_con, table_creation_sql)

            #Fetch rows from table to ensure not already populated:
            sql = "select * from {}".format(table_name)
            if table_name not in ['sqlite_sequence']:
                stmt = ibm_db.exec_immediate(db2_con, sql)
                res_tuple = ibm_db.fetch_tuple(stmt)
                if not res_tuple:
                    print(table_name)
                    # Populate DB2 tables with sqlite data
                    query = """SELECT * FROM {}"""
                    print(query.format(table_name))
                    res = cur.execute(query.format(table_name))
                    val_rs = res.fetchall()
                    val_rs_df = pd.DataFrame(val_rs, columns = col_rs_df[1])
                    # print(val_rs_df)
                    statements = build_insert_sql(table_name, val_rs_df, col_rs_df)
                    stmt_ix = 0
                    for i in range(0, len(statements)):
                        statement = statements[stmt_ix]
                        try:
                            print("Trying to insert into Schema:", schema_name, statement)
                            ibm_db.exec_immediate(db2_con, statement)
                            stmt_ix += 1
                        except:
                            print("Skipping insert of:", schema_name, statement)
                            skip_log.write(schema_name + " | " + table_name + " | " + statement + "\n")
                            stmt_ix += 1

skip_log.close()





