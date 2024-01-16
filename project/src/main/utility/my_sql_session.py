from mysql import mysql.connector

conn = mysql.connector.connect(user = 'root',
							host = '127.0.0.1:3306',
							database = 'Myconn')

print(conn)

