from clickhouse_driver import Client
client = Client(host='localhost',user="default",password="nur")


sqlCreateClickhouseTable = """
            CREATE TABLE pythondb.customers4 (
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

sqlConnectToKafka = """ CREATE TABLE pythondb.customers_queue4 (
        operation_type String,
        db String,
        table String,
        createdAt UInt64,
        name String,
        address String,
        id UInt64
    ) ENGINE = Kafka 
       SETTINGS kafka_broker_list = '127.0.0.1:9092',
       kafka_topic_list = 'dataSyncMysqlClickhouseTest2',
       kafka_group_name = 'group1',
       kafka_format = 'JSONEachRow';
       """

sqlMaterializedView = """
        CREATE MATERIALIZED VIEW pythondb.customers_queue_mv4 TO pythondb.customers4 AS
            SELECT *
            FROM pythondb.customers_queue4;

"""

result = client.execute(sqlCreateClickhouseTable)
print(result)


result = client.execute(sqlConnectToKafka)
print(result)


result = client.execute(sqlMaterializedView)
print(result)