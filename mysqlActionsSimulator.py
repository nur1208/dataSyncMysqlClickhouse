
from faker import Faker
import time
from utils import execSQL, getConfig, printMessage

fake = Faker()


def getRamdomData(db):
    sql = "select * from customers ORDER BY RAND() LIMIT 1;"
    cursor = execSQL(db, sql)
    return cursor.fetchone()

def insertData(db):
    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = (fake.name(), fake.address())
    execSQL(db, sql, val)
    printMessage("Data insterted to mysql ➕")

def updateData(db):
    randomData = getRamdomData(db)
    sql = "UPDATE customers SET address = %s WHERE address = %s"
    val = (fake.address(), randomData[1])
    execSQL(db, sql, val)
    printMessage(f"data with {randomData[2]} id updated 🆗")



def deleteData(db):
    randomData = getRamdomData(db)
    sql = "DELETE FROM customers WHERE id = %s"
    val = (randomData[2], )
    execSQL(db, sql, val)
    printMessage(f"data with {randomData[2]} id deleted 🆗")


# def deleteData(db):


def runAction(type):
    db = getConfig()
    speed = 3

    if(type == "insert"):
        printMessage("mysql actions simulator started ▶")
        while True:
            insertData(db)
            time.sleep(2 * speed)
    elif (type == "update"):
        while True:
            updateData(db)
            time.sleep(3 * speed)
    elif type == "delete":
        while True:
            deleteData(db)
            time.sleep(4 * speed)
        
