from clickhouse_driver import Client

from utils import convertMysqlDataTypeClickhouse, createClickhouseSchema, execSQL, getConfig, printMessage

client = Client(host='localhost',user="default",password="nur")

def clickhouseConsumer(db, table,mysqlSchema,topic , group, 
    kafkaServer ="127.0.0.1:9092"):

    [clickhouseSchema, primary_key] = createClickhouseSchema(mysqlSchema)
    
    if(primary_key == ''): 
        printMessage(f"didn't find primary key from mysqlSchema ❌")
        return False

    sqlCreateClickhouseTable = f"""
                CREATE TABLE IF NOT EXISTS {db}.{table} (
                    {clickhouseSchema}
                ) Engine = MergeTree
                ORDER BY (createdAt, {primary_key})
    """

    printMessage("clickhouse Schema:")
    print(sqlCreateClickhouseTable)
    sqlConnectToKafka = f""" CREATE TABLE IF NOT EXISTS {db}.{table}_queue (
            {clickhouseSchema}
        ) ENGINE = Kafka 
        SETTINGS kafka_broker_list = '{kafkaServer}',
        kafka_topic_list = '{topic}',
        kafka_group_name = '{group}',
        kafka_format = 'JSONEachRow';
        """

    sqlMaterializedView = f"""
            CREATE MATERIALIZED VIEW  IF NOT EXISTS {db}.{table}_queue_mv TO {db}.{table} AS
                SELECT *
                FROM {db}.{table}_queue;

    """

    sqlCreateDatabaseIfNotExists = f"""
        CREATE DATABASE IF NOT EXISTS {db}
    """

    try:
        client.execute(sqlCreateDatabaseIfNotExists)
        printMessage("create database if not exists ✅")

        
        client.execute(sqlCreateClickhouseTable)
        printMessage("clickhouse table created successfully if not exists ✅")

        client.execute(sqlConnectToKafka)
        printMessage("connected to kafka successfully ✅")


        client.execute(sqlMaterializedView)
        printMessage("transfer data between Kafka and the clickhouse table ✅")


        return True
    except Exception as e:
        print(e)
        printMessage("something went wrong while creating clickhouse table and connected to kafka ❌")
        return False

if __name__ == "__main__":

    db = getConfig()
    columnsName = execSQL(db,"DESCRIBE customers").fetchall()
    
    # [clickhouseSchema, primary_key] = createClickhouseSchema(columnsName)
    
    # print(clickhouseSchema)
    # print(primary_key)

    clickhouseConsumer("pythondb", "customers12", 
        columnsName,"dataSyncMysqlClickhouseTest2", "group6")