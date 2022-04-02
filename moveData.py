from kafka import KafkaProducer
from utils import execSQL, getConfig, printMessage
import json
import time


def moveData():
    def json_serializer(data):
        return json.dumps(data).encode("utf-8")

    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=json_serializer)

    db = getConfig()

    cursor = execSQL(db,"select * from customers")
    result = cursor.fetchall()
    columnsName = execSQL(db,"DESCRIBE customers").fetchall()

    printMessage("starting moving data to kafka:")
    for x in result:
        data= dict()
        data['operation_type'] = 'INSERT'
        data['db'] = 'pythondb'
        data['table'] = 'customers'
        milliseconds = int(time.time() * 1000)
        data["createdAt"] = milliseconds
        
        for index in range(0, len(x)):
            data[columnsName[index][0]] = x[index]

        producer.send("dataSyncMysqlClickhouseTest2",data)
    printMessage("done moving data to kafka ✅")

if __name__ == "__main__":
    moveData()



