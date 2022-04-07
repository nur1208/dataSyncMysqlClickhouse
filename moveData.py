from kafka import KafkaProducer
from utils import execSQL, getConfig, printMessage
import json
import time


def moveData(db, mysqlTable, kafkaTopic):
    try:
        def json_serializer(data):
            return json.dumps(data).encode("utf-8")

        producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                            value_serializer=json_serializer)

        # db = getConfig(mysqlDB)

        cursor = execSQL(db,f"select * from {mysqlTable}")
        result = cursor.fetchall()
        columnsName = execSQL(db,f"DESCRIBE {mysqlTable}").fetchall()

        printMessage("starting moving data to kafka:")
        for x in result:
            data= dict()
            data['operation_type'] = 'INSERT'
            milliseconds = int(time.time() * 1000)
            data["createdAt"] = milliseconds
            
            for index in range(0, len(x)):
                data[columnsName[index][0]] = x[index]

            producer.send(kafkaTopic,data)
        printMessage("done moving data to kafka ✅")

        return True
    except Exception as e:
        printMessage(e)
        return False

if __name__ == "__main__":
    moveData()



