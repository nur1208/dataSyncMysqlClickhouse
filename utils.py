import configparser
import time
import pymysql


def execSQL(db, sql, value = False):
    cursor = db.cursor()
    if(value):
        if(isinstance(value, list)):
            print("here")
            cursor.executemany(sql, value)
        else:
            cursor.execute(sql, value)
    else:
        cursor.execute(sql)
    db.commit()
    # data = cursor.fetchone()
    # print(time.strftime('[%H:%M:%S]:  ') + str(data ))
    return  cursor

def printMessage(message):
    print(time.strftime('[%H:%M:%S]') +" "+message)

def getConfig():
    conf = configparser.ConfigParser()
    # print(conf)
    try:
        conf.read("config.ini")
        host = "127.0.0.1"
        port = 3307
        user = "test"
        password = "12345678"
        db_name = "pythonDB"
        charset = "utf8"
        print(time.strftime('[%H:%M:%S]') + " Configuration succeed. ✅")
    except:
        print(time.strftime('[%H:%M:%S]') + " Configuration failed. ❌")

    try:
        # global db
        db = pymysql.connect(host=host,user=user,password=password,database=db_name,port=port,charset=charset)
        print(time.strftime('[%H:%M:%S]') + ' Database connection succeed. ✅')
        return db

    except:
        print(time.strftime('[%H:%M:%S]') + ' Database connection failed ❌')