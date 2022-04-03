from clickhouse_driver import Client

from utils import convertMysqlDataTypeClickhouse, execSQL, getConfig, printMessage

client = Client(host='localhost',user="default",password="nur")

def clickhouseConsumer(db, table,topic , group, 
    kafkaServer ="127.0.0.1:9092"):


    sqlCreateClickhouseTable = f"""
                CREATE TABLE {db}.{table} (
                    operation_type String,
                    db String,
                    table String,
                    createdAt UInt64,
                    name String,
                    address String,
                    id UInt64
                ) Engine = MergeTree
                ORDER BY (id, createdAt)

    """

    sqlConnectToKafka = f""" CREATE TABLE {db}.{table}_queue (
            operation_type String,
            db String,
            table String,
            createdAt UInt64,
            name String,
            address String,
            id UInt64
        ) ENGINE = Kafka 
        SETTINGS kafka_broker_list = '{kafkaServer}',
        kafka_topic_list = '{topic}',
        kafka_group_name = '{group}',
        kafka_format = 'JSONEachRow';
        """

    sqlMaterializedView = f"""
            CREATE MATERIALIZED VIEW {db}.{table}_queue_mv TO {db}.{table} AS
                SELECT *
                FROM {db}.{table}_queue;

    """
    try:
        client.execute(sqlCreateClickhouseTable)
        printMessage("clickhouse table created successfully ✅")

        client.execute(sqlConnectToKafka)
        printMessage("connected to kafka successfully ✅")


        client.execute(sqlMaterializedView)
        printMessage("transfer data between Kafka and the clickhouse table ✅")


    except Exception as e:
        print(e)
        printMessage("something went wrong while creating clickhouse table and connected to kafka ❌")

if __name__ == "__main__":

    db = getConfig()
    columnsName = execSQL(db,"DESCRIBE customers").fetchall()

    print(columnsName)
    for column in columnsName:
        primary_key = ''
        if(column[3] == "PRI"):
            primary_key = column[0]
            print(primary_key)
        datatype = convertMysqlDataTypeClickhouse(column[1])
        print(f'datatype = {datatype}, column={column[1]}')


    # clickhouseConsumer("pythondb", "customers8", 
    #     "dataSyncMysqlClickhouseTest2", "group4")