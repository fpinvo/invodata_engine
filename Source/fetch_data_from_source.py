#!/usr/bin/env python
import numpy as np
from odo import odo
import pandas as pd
import sys
import sqlalchemy as sqlalchemy

import csv
import os

from Drivers.mysql.mysql_driver import MySqlDriver

# import config

'''
This module description would be similar to the class description
'''


class FetchDataFromSource:
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

    def fetch_tables(self, conn):
        tables_name = []
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        for table_name in cursor:
            tables_name.append(''.join(table_name))
        self.get_data(conn, tables_name)
        cursor.close()

    def get_data(self, conn, tables):
        for table in tables:
            cursor = conn.cursor()
            cursor.execute("Select * From " + table + ";")
            result = cursor.fetchall()
            num_fields = len(cursor.description)
            field_names = [i[0] for i in cursor.description]
            print(field_names)
            # Write result to file.
            with open('/var/lib/mysql-files/' + table + '.csv', 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)

                # csvwriter.writerow(field_names)
                for value in result:
                    csvwriter.writerow(value)
            cursor.close()

# if __name__ == '__main__':
#     f_data = FetchDataFromSource()
#     f_data_con = f_data.test_connection(config.server, config.username, config.password, config.database, config.port)
#     f_data.fetch_tables(f_data_con)

