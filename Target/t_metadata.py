#!/usr/bin/python
import json
import sys

from Source import config
from Drivers.mysql.mysql_driver import MySqlDriver

'''
This module description would be similar to the class description
'''


class TargetMetaData:
    """
    Class Description
    """

    # data_member_1: str
    # data_member_2: str

    def __init__(self):
        pass

    # test the mysql connection and get the cursor
    def test_connection(self, server, username, password, database, port):
        mysql_connection = MySqlDriver(server, username, password, database, port)
        test_conn = mysql_connection.mysql_conn
        return test_conn

    def create_db(self, cur):
        db_names = config.database
        mycursor = cur.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS " + db_names + "_invodata ;")

    def create_table(self, cur):
        source_schema_file = json.load(open('Source/Source_schema.json'))
        len_table_list = len(source_schema_file['Query_set'])
        print(len_table_list)
        list_keys = []
        db_names = config.database
        print(db_names)
        for index in range(len_table_list):

            for table_query_key, table_query_value in source_schema_file['Query_set'][index].items():
                print(table_query_key, table_query_value[0])
                try:
                    mycursor = cur.cursor(buffered=True)
                    mycursor.execute("USE " + db_names + "_invodata ;")
                    mycursor.execute(table_query_value[0] + ";")
                except Exception as e:
                    print(e)
                    list_keys.append(index)
                finally:
                    mycursor.close()

        return mycursor

# if __name__ == '__main__':
#     t = TargetMetaData()
#     cur = t.test_connection(config.server, config.username, config.password, config.database, config.port)
#     t.create_db(cur)
#     target_table = t.create_table(cur)
#     target_table.close()
#     cur.close()
# "localhost", "root", "root", "classicmodels", 3306
