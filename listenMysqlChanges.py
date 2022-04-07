import time

from canal.client import Client
from canal.protocol import EntryProtocol_pb2
from canal.protocol import CanalProtocol_pb2

from utils import createKafkaProducer, printMessage

client = Client()
client.connect(host='127.0.0.1', port=11111)
client.check_valid(username=b'canals', password=b'12345678')
client.subscribe(client_id=b'1', destination=b'example', filter=b'.*\\..*')

producer = createKafkaProducer()
def listeningToMysqlChanges(dbName, table, topic):
    printMessage(f"starting listening to mysql binlog for {dbName}.{table} ✅")
    while True:
        message = client.get(100)
        entries = message['entries']
        for entry in entries:
            entry_type = entry.entryType
            if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                continue
            row_change = EntryProtocol_pb2.RowChange()
            row_change.MergeFromString(entry.storeValue)
            event_type = row_change.eventType
            header = entry.header
            databaseCurrent = header.schemaName
            tableCurrent = header.tableName
            event_type = header.eventType

            if dbName != databaseCurrent or table !=tableCurrent:
                continue
            for row in row_change.rowDatas:
                format_data_2 = dict()
                format_data_2["createdAt"] = int(time.time() * 1000)
                if event_type == EntryProtocol_pb2.EventType.DELETE:
                    for column in row.beforeColumns:
                        format_data_2[column.name] =column.value
                    format_data_2["operation_type"] = "DELETE"
               
                elif event_type == EntryProtocol_pb2.EventType.INSERT:
                    format_data_2["operation_type"] = "INSERT"
                    for column in row.afterColumns:
                        format_data_2[column.name] =column.value
                else:
                    format_data_2["operation_type"] = "UPDATE"

                    for column in row.afterColumns:
                        format_data_2[column.name] = column.value

                printMessage(f'action: {format_data_2["operation_type"]}, database: {databaseCurrent}, table: {tableCurrent}')

                producer.send(topic,format_data_2)

        # time.sleep(1)

    client.disconnect()

if __name__ == "__main__":
    listeningToMysqlChanges("pythondb", "customers", 
        "dataSyncMysqlClickhouseTest2")