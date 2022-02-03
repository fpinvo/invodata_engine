#!/usr/bin/python
import json
import sys

import Source.config as config
from Drivers.mysql.mysql_driver import MySqlDriver

'''
This module description would be similar to the class description
'''


class SourceMetaData:
    """
    Class Description
    """

    def __init__(self):
        pass

    # test the mysql connection and get the cursor
    def test_connection(self, server, username, password, database, port):
        mysql_connection = MySqlDriver(server, username, password, database, port)
        test_conn = mysql_connection.mysql_conn
        return test_conn

    # def get_db_name(self, cur):
    #     mycursor = cur.cursor()
    #     mycursor.execute("""SELECT schema_name FROM information_schema.schemata WHERE schema_name  NOT IN ('information_schema', 'sys','mysql','performance_schema');""")
    #     return [''.join(x) for x in mycursor]

    # get the tables of the database
    def get_table_names(self, cur, db):
        mycursor = cur.cursor()
        mycursor.execute("SHOW TABLES FROM " + db + ";")
        return mycursor

    def sort_the_relations(self,tables):
        list_sorted_tables_relations = []
        tables_name_done_with = []
        for table in tables:
            for table_key, table_value in table.items():
                if not list_sorted_tables_relations:
                    list_sorted_tables_relations.append(table)
                    tables_name_done_with.append(table_key)
                elif any(table_key in str(s) for s in list_sorted_tables_relations):
                    print(table_key)
                    current_key = tables_name_done_with.index(pre_key)
                    tables_name_done_with.insert(current_key, table_key)
                    list_sorted_tables_relations.insert(current_key, table)
                elif "FOREIGN KEY" not in str(table_value):
                    tables_name_done_with.insert(0,table_key)
                    list_sorted_tables_relations.insert(0,table)
                else:
                    tables_name_done_with.append(table_key)
                    list_sorted_tables_relations.append(table)
                pre_key = table_key


        print(list_sorted_tables_relations[0])
        print(tables_name_done_with)
        # self.json_file(json.dumps(list_sorted_tables_relations))
        self.convert_in_json_structure(list_sorted_tables_relations)

    # get the schema and metadata of table
    def get_metadata(self, s_table):
        list_tables = []
        table_names = [''.join(x) for x in s_table]
        query_set = {"Query_set": []}
        for name in table_names:
            s_table.execute("show create table " + name)
            # convert dic to json
            for table in s_table:
                list_tables.append({table[0]: [table[1].replace("\n", "")]})

        self.sort_the_relations(list_tables)

    def convert_in_json_structure(self,s_table):
        query_set = {"Query_set": []}
        for table in s_table:
            for table_key, table_value in table.items():
                query_set["Query_set"].append({table_key: table_value})

        self.json_file(query_set)

    # store into json file
    def json_file(self, query_set):
        with open("./Source/Source_schema.json", "w") as outfile:
            json.dump(query_set, outfile)


# if __name__ == '__main__':
#     s = SourceMetaData()
#     cur = s.test_connection(config.server, config.username, config.password, config.database, config.port)
#     source_table = s.get_table_names(cur, config.database)
#     source_metadata = s.get_metadata(source_table)
#     # print(source_metadata)
#     source_table.close()
#     cur.close()
