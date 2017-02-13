import mysql.connector
from mysql.connector import errorcode

class mysql_storer(object):
    def __init__(self, id, pw):
        self.cnx = mysql.connector.connect(user=id, password=pw)
        self.cursor = self.cnx.cursor()

    def create_database(self, dbName):
        try:
            self.cursor.execute(
                "CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(dbName)
            )
        except mysql.connector.Error as err:
            print "Failed creating database: {}".format(err)

        self.cnx.database = dbName

    def create_table(self, tbString):
        try:
            self.cursor.execute(tbString)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print ('already exists.')
            else:
                print (err.msg)

    def drop_table(self, tbName):
        query = "DROP TABLE IF EXISTS " + tbName
        self.cursor.execute(query)

    def insert_data(self, insertTemplateString, insertDataString):
        self.cursor.execute(insertTemplateString, insertDataString)

    def update(self, updateTemplate, updateData):
        self.cursor.execute(updateTemplate, updateData)

    def check_exist(self, tbName, column, value):
        print tbName, column, value
        query = "SELECT COUNT(1) FROM " + tbName + " WHERE " + column + " = " + value
        print query
        self.cursor.execute(query)
        if self.cursor.fetchone()[0]:
            return 1
        else:
            return 0

    def close(self):
        self.cnx.commit()
        self.cursor.close()
        self.cnx.close()