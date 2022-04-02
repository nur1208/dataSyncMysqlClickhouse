
from clickhouse_driver import Client
from kafka import KafkaConsumer
import json

from utils import convertMysqlDataTypeClickhouse, execSQL, getConfig

client = Client(host='localhost',user="default",password="nur")
def create_consumer(topic, groupId):
    return KafkaConsumer(
        topic,
        bootstrap_servers='127.0.0.1:9092',
        auto_offset_reset='earliest',
        group_id=groupId)

# result = client.execute('SHOW DATABASES')
# result = client.execute("SELECT name, comment FROM system.databases WHERE name = 'db_comment';")
# result = client.execute("CREATE DATABASE pythondb ENGINE = Memory COMMENT 'test data sync';")

result = client.execute('select * from pythondb.customers4')

# sql="ALTER TABLE `%s`.`%s` DELETE WHERE id = %s ;" %("pythondb" ,
#         binlogevent.table,binlogevent.primary_key,
#         info[binlogevent.primary_key])


if __name__ == "__main__":
    # consumer = create_consumer("dataSyncMysqlClickhouse","consumer-group-a")
    # print("starting the consumer")
    # values = ''
    # for msg in consumer:
    #     data = json.loads(msg.value)
    #     print("data = {}".format(data))
    #     values = values + f"({msg.value[0]},{msg.value[1]},{msg.value[2]})"
    #     # print(values)
    #     print(type(data["id"]))
    #     client.execute('INSERT INTO test1 (*) VALUES',  [{'x': data["id"]}, {'x': 2}, {'x': 3}, {'x': 100}])
    db = getConfig()
    columnsName = execSQL(db,"DESCRIBE test4").fetchall()

    for column in columnsName:
        datatype = convertMysqlDataTypeClickhouse(column[1])
        print(f'datatype = {datatype}, column={column[1]}')

