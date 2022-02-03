#!/usr/bin/python

from mysql.connector import MySQLConnection , Error

'''
This mysql driver module help us to connect to the mysql and do the operations
'''


class MySqlDriver:
    """
    Class Description
    Mysql class get the credentials of database and the text the connection first then establish it.
    """

    # run as constructor and set all the paramiters
    def __init__(self, Server, Username=None, Password=None, Database=None, Port=None):

        self.Server = Server
        self.Username = Username
        self.Password = Password
        self.Database = Database
        self.Port = Port

    # mysql_conn test the connection and return a cursor
    @property
    def mysql_conn(self):
        try:
            if self.Port is None:
                self.Port = 3306
            mydb = MySQLConnection(
                host=self.Server,
                user=self.Username,
                password=self.Password,
                database=self.Database,
                port=self.Port
            )
            if mydb.is_connected():
                print('Connected to MySQL database')
                print("---- CONNECTION SUCCESS ----")
            return mydb
        except Error as e:
            print(e)


