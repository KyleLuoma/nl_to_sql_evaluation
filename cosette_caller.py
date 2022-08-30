# Interact with the Cosette SQL prover API to test semantic equivalence
# between our codex generated and spider gold queries.
# API guide: http://cosette.cs.washington.edu/guide#api

import db_connector
import get_all_codex_queries_from_sqlite_db as get_codex
import json
import requests

spider_root = "//192.168.1.17/data/nl_benchmarks/spider/"


def call_cosette(program):

    response = requests.post(
        'https://demo.cosette.cs.washington.edu/solve',
        data={"api_key":"a6813477a2ef7682aca811786f8db77e", "query":"{}".format(program)},
        verify=False
        )

    print(response.json()['result'])
    if response.json()['result'] == 'ERROR':
        print(response.json()['error_msg'])

    # Response format:
    # {
    #     "result": result,                   // can be EQ, NEQ, UNKNOWN, or ERROR
    #     "counterexamples": counterexamples, // counterexample if result is NEQ
    #     "coq_source": coq_source,           // generated Coq source code
    #     "rosette_source": rosette_source,   // generated Rosette source code
    #     "error_msg": error_msg,             // error message if result = ERROR
    # }


def get_names_in_query(query_attributes):
    select_cols = query_attributes['select'][1]
    col_in_query = []
    col_names = db_tables[db_id]['column_names_original']
    print("SELECT COLUMNS:")
    for column in select_cols:
        col_id = column[1][1][1]
        col_name = col_names[col_id][1]
        print("Column ID:", col_id, "Name:", col_name)
        col_in_query.append(col_name)

    
    if len(query_attributes['where']) > 0:
        where_cols = query_attributes['where']
        print("WHERE COLUMNS:")
        for column in where_cols:
            if type(column) == list:
                col_id = column[2][1][1]
                col_name = col_names[col_id][1]
                print("Column ID:", col_id, "Name:", col_name)
                col_in_query.append(col_name)

    if len(query_attributes['groupBy']) > 0:
        gb_cols = query_attributes['groupBy']
        print("GROUP BY COLUMNS:")
        for column in gb_cols:
            col_id = column[1]
            col_name = col_names[col_id][1]
            print("Column ID:", col_id, "Name", col_name)
            col_in_query.append(col_name)

    if len(query_attributes['orderBy']) > 0:
        ob_cols = query_attributes['orderBy'][1]
        print("ORDER BY COLUMNS:")
        for column in ob_cols:
            col_id = column[1][1]
            col_name = col_names[col_id][1]
            print("Column ID:", col_id, "Name", col_name)
            col_in_query.append(col_name)
    
    return set(col_in_query)

if __name__ == "__main__":

    f = open('.local/cosette.json')
    cosette_json = json.load(f)
    f.close()

    f = open(spider_root + 'dev.json')
    dev_json = json.load(f)
    f.close()

    f = open(spider_root + 'tables.json')
    tables_json = json.load(f)
    f.close()

    db_tables = {}
    for db in tables_json:
        db_tables[db['db_id']] = db

    codex_queries = get_codex.as_dataframe()

    q_ct = 0
    for row in codex_queries.itertuples():
        db_id = dev_json[q_ct]['db_id']

        table_units = dev_json[q_ct]['sql']['from']['table_units']
        tables = {}

        # Extract from base query
        if len(table_units) == 1 and type(table_units[0][1]) != dict:   
            table_unit = dev_json[q_ct]['sql']['from']['table_units'][0][1]
            table_name = db_tables[db_id]['table_names_original'][table_unit]

            gold_query = dev_json[q_ct]['query']

            print("\n\n", gold_query)
            print("Table Name:", table_name)

            query_attributes = dev_json[q_ct]['sql']

            names = get_names_in_query(query_attributes=query_attributes)
            tables[table_name] = names
            print(tables)

        # Extract from intersect
        if len(table_units) == 1 and dev_json[q_ct]['sql']['intersect'] != None and len(dev_json[q_ct]['sql']['intersect']) > 0:
            intersect_table_unit = dev_json[q_ct]['sql']['intersect']['from']['table_units'][0][1]
            intersect_table_name = db_tables[db_id]['table_names_original'][intersect_table_unit]

            intersect_query_attributes = dev_json[q_ct]['sql']['intersect']
            print("INTERSECT TABLES AND COLUMNS:")
            intersect_names = get_names_in_query(intersect_query_attributes)
            if table_name == intersect_table_name:
                tables[intersect_table_name] = set(list(tables[intersect_table_name]) + list(intersect_names))
            else:
                tables[intersect_table_name] = intersect_names

            print(tables)

        # Extract from union
        if len(table_units) == 1 and dev_json[q_ct]['sql']['union'] != None and len(dev_json[q_ct]['sql']['union']) > 0:
            intersect_table_unit = dev_json[q_ct]['sql']['union']['from']['table_units'][0][1]
            intersect_table_name = db_tables[db_id]['table_names_original'][intersect_table_unit]

            intersect_query_attributes = dev_json[q_ct]['sql']['union']
            print("UNION TABLES AND COLUMNS:")
            intersect_names = get_names_in_query(intersect_query_attributes)
            if table_name == intersect_table_name:
                tables[intersect_table_name] = set(list(tables[intersect_table_name]) + list(intersect_names))
            else:
                tables[intersect_table_name] = intersect_names

            print(tables)

        # Extract from subquery
        where_statements = dev_json[q_ct]['sql']['where']
        if len(where_statements) > 0:
            print("SUBQUERY TABLES AND COLUMNS:")
            for statement in where_statements[0]:
                if type(statement) == dict and 'from' in statement.keys():
                    where_table_id = statement['from']['table_units'][0][1]
                    where_table_name = db_tables[db_id]['table_names_original'][where_table_id]
                    where_column_names = get_names_in_query(statement)
                    if where_table_name in tables.keys():
                        tables[where_table_name] = set(list(tables[where_table_name]) + list(where_column_names))
                    else:
                        tables[where_table_name] = where_column_names
                    print(tables)

        codex_query = row.codex_query

        # Do table name dotted id generation for both queries:
        for table in tables:
            for column in tables[table]:
                gold_query = gold_query.replace(column, table + '.' + column)
                codex_query = codex_query.replace(column, table + '.' + column)
        print('GOLD QUERY:', gold_query)
        print('CODEX QUERY:', codex_query)

        # Cosette schema definitions
        schemas = {}
        sqlite_db = db_connector.sqlite_connector(spider_root + 'database/' + db_id + '/' + db_id + '.sqlite')
        for table in tables:
            column_df = sqlite_db.do_query("pragma table_info('{}')".format(table))
            column_df['name'] = column_df.apply(
                lambda row: row['name'].strip(),
                axis = 1
            )
            column_df.set_index('name', inplace=True)
            # Discover column types and build schema dictionary:
            columns_and_types = []
            for column in tables[table]:
                if column != '*':
                    d_type = 'str'
                    try:
                        sqlite_type = column_df.loc[column.strip()]['type']
                        if 'INT' in sqlite_type.upper():
                            d_type = 'int'
                        if 'REAL' in sqlite_type.upper():
                            d_type = 'float'
                        if 'BOOL' in sqlite_type.upper():
                            d_type = 'bool'
                    except:
                        d_type = 'str'
                    columns_and_types.append(tuple((column, d_type)))
            schemas[table] = columns_and_types
        print(schemas)
        program = ""
        for schema in schemas:
            program = program + 'schema schema_' + schema + '('
            if len(schemas[schema]) == 0:
                program += '??);\n'
            else:
                for column in schemas[schema]:
                    program += (column[0] + ':' + column[1] + ', ')
                program += ' ??);\n\n'

        for table in tables:
            program += ('table ' + table + '(schema_' + table + ');\n')

        program += '\nquery q1 '
        program += ('`' + gold_query + '`;\n')
        program += '\nquery q2 '
        program += ('`' + codex_query + '`;\n')
        program += ('\nverify q1 q2;')

        print(program)

        q_ct += 1


    cosette_program = """
    schema s1(Singer_ID:int, Name:str, Country:str);       -- schema declaration

    table singer(s1);                   -- table a of schema s1

    query q1                       -- query 1 on table a
    `SELECT s.Singer_ID FROM singer s`;

    query q2                       -- query 2 on table a
    `SELECT s.Name FROM singer s`;

    verify q1 q2;                  -- does q1 equal to q2?
    """



