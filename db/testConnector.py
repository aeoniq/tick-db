import mysql.connector

cnx = mysql.connector.connect(user='peerapong', password='pdhacc',
                              host='127.0.0.1',
                              database='testdb')

cursor = cnx.cursor()
#execfile('create_tick_data_tables.py')
import create_tick_data_tables

create_tick_data_tables.create_all_tables(cursor)
cursor.close()
cnx.close()
