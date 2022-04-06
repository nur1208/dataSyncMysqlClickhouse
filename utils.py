import configparser
import time
import pymysql
import re
from kafka import KafkaProducer
import json

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

def createKafkaProducer():
    def json_serializer(data):
        return json.dumps(data).encode("utf-8")

    return KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=json_serializer)

def createClickhouseSchema(mysqlSchema):
    primary_key = ''
    clickhouseSchema = 'createdAt UInt64,operation_type String,'
    for column in mysqlSchema:
        if(column[3] == "PRI"):
            primary_key = column[0]
        datatype = convertMysqlDataTypeClickhouse(column[1])
        clickhouseSchema =clickhouseSchema + f'{column[0]} {datatype},'
    
    clickhouseSchema = clickhouseSchema[:-1]
    return [clickhouseSchema, primary_key]


def convertMysqlDataTypeClickhouse(datatype):
    processedDatattype = re.sub("\(.*\)", "", datatype.upper())

    if(processedDatattype == "TINYINT UNSIGNED"):
        return "UInt8"
    elif(processedDatattype == "SMALLINT UNSIGNED"):
        return "UInt16"
    elif(processedDatattype == "INT UNSIGNED"
        or processedDatattype == "MEDIUMINT UNSIGNED"
    ):
        return "UInt32"
    elif(processedDatattype == "BIGINT UNSIGNED"):
        return "UInt64"

    
    elif(processedDatattype == "TINYINT"):
        return "Int8"
    elif(processedDatattype == "SMALLINT"):
        return "Int16"
    elif(processedDatattype == "INT"
        or processedDatattype == "MEDIUMINT"
    ):
        return "Int32"
    elif(processedDatattype == "BIGINT"):
        return "Int64"
    
    
    elif(processedDatattype == "FLOAT"):
        return "Float32"
    elif(processedDatattype == "DOUBLE"
        or processedDatattype == "DECIMAL"):
        return "Float64"

    elif(processedDatattype == "BLOB" 
        or processedDatattype == "TINYTEXT"
        or processedDatattype == "MEDIUMTEXT"
        or processedDatattype == "LONGTEXT"
        or processedDatattype == "TINYBLOB"
        or processedDatattype == "MEDIUMBLOB"
        or processedDatattype == "LONGBLOB"
        or processedDatattype == "TEXT"
        or processedDatattype == "VARBINARY"
        or processedDatattype == "VARCHAR"):
        return "String"

    
    elif(processedDatattype == "CHAR" 
        or processedDatattype == "BINARY"):
        return "FixedString(32)"
    
    elif(processedDatattype == "DATE"):
        return "Date"

    elif(processedDatattype == "DATETIME" 
        or processedDatattype == "TIME"
        or processedDatattype == "YEAR"
        or processedDatattype == "TIMESTAMP"):
        return "DateTime"
    
    elif(processedDatattype == "ENUM"):
        return "Enum"
    elif(processedDatattype == "SET"):
        return "Set"
    

    
    
    return "didn't find ❌"
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