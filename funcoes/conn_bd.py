import mysql.connector

def connect():
    mydb = mysql.connector.connect(
    host="localhost",
    user="opera",
    password="tgbhyt@iolAXr",
    database="projeto_tcc2")
    mydb.cursor()
    return mydb


