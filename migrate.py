import os
import json
import sqlite3
from this import d
import ibm_db
import psycopg2
import pandas as pd
from collections import defaultdict
import db_connector

# os.chdir(spider_root)

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/database/"           
POSTGRES_RESERVED_WORDS = ['cast', 'user', 'end', 'from']


def build_table_creation_sql(table_name, col_df, db_type = 'db2'):
    print("COLUMN DF:", col_df)
    primary_keys = col_df.where(col_df[5] >= 1).dropna(how = "all")

    DOUBLE = 'DOUBLE'
    BLOB = 'BLOB'
    if db_type == 'postgresql':
        DOUBLE = 'double precision'
        BLOB = 'bytea'
    
    sql = "CREATE TABLE IF NOT EXISTS " + table_name + "(\n"

    for col in col_df.itertuples():
        column_name = col[2]
        if (
            "%" in column_name 
            or "(" in column_name 
            or ")" in column_name 
            or " " in column_name
            or column_name[0].isnumeric()
            or (db_type == 'postgresql' 
                and column_name.lower() in POSTGRES_RESERVED_WORDS)
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
            data_type = DOUBLE
        if "FLOAT" in data_type:
            data_type = DOUBLE
        if "DOUBLE" in data_type:
            data_type = DOUBLE
        if "DATE" in data_type:
            data_type = "VARCHAR(127)"
        if "YEAR" in data_type:
            data_type = "INTEGER"
        if "BLOB" in data_type:
            data_type = BLOB
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
    spider_root = "//192.168.1.17/data/nl_benchmarks/spider/database/"
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

# supported target_db_types: postgresql db2
# schema_only: only do table creation; does not do value insertions if true.
# start_with_db: skip sqlite databases before this in source folder
# sqlite_to_level options: 
#    - schema: create a schema in default DB for each sqlite database
#    - db: create a database for each sqlite database
def do_migration(spider_root, target_db_type = 'db2', schema_only = False, 
                 start_with_db = 'academic', sqlite_to_level = 'schema'):

    if target_db_type == 'db2':
        target_con = db_connector.db2_connector()
    elif target_db_type == 'postgresql':
        target_con = db_connector.postgresql_connector()

    assert sqlite_to_level in ['schema', 'database']

    table_retrieval = """
    select * from sqlite_schema where type = 'table' order by name;
    """

    column_info = """
    pragma table_info('{}')
    """

    # Use to skip already completed dbs:
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
                print(schema_name)
                if schema_name == start_with_db:
                    do_create = True
                if not do_create:
                    continue

                # Create schema
                try:
                    target_con.do_query("create schema " + schema_name)
                except:
                    print(sqlite_to_level, schema_name, "already exists")

                if target_db_type == 'db2':
                    set_schema = "set current schema = "
                elif target_db_type == 'postgresql':
                    set_schema = "SET search_path = "
                print(set_schema + schema_name)
                target_con.do_query(set_schema + schema_name)

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
                if target_db_type == 'postgresql' and table_name.lower() in POSTGRES_RESERVED_WORDS:
                    table_name = '"' + table_name + '"'
                print(" ---- SCHEMA ", schema_name, " TABLE ", table_name, " ---- ")
                res = cur.execute(column_info.format(table_name.replace('"', '')))
                col_rs = res.fetchall()
                col_rs_df = pd.DataFrame(col_rs)
                # print(col_rs_df)
                table_creation_sql = build_table_creation_sql(table_name, col_rs_df, target_db_type)
                # print(table_creation_sql)

                if table_name != 'sqlite_sequence':
                    try:
                        target_con.do_query("drop table " + table_name)
                    except:
                        pass
                    print(table_creation_sql)
                    target_con.do_query(table_creation_sql)

                #Fetch rows from table to ensure not already populated:
                sql = "select * from {}".format(table_name)
                if table_name not in ['sqlite_sequence']:
                    res_df = target_con.do_query(sql)
                    if res_df.shape[0] == 0 and not schema_only:
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
                                target_con.do_query(statement)
                                stmt_ix += 1
                            except:
                                print("Skipping insert of:", schema_name, statement)
                                skip_log.write(schema_name + " | " + table_name + " | " + statement + "\n")
                                stmt_ix += 1

    skip_log.close()
    target_con.close_connection()

if __name__ == "__main__":
    do_migration(spider_root, 'postgresql', False, start_with_db='academic')



