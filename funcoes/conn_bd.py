import mysql.connector

def connect():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="projeto_tcc2")
    mydb.cursor()
    return mydb


