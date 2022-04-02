import time

from canal.client import Client
from canal.protocol import EntryProtocol_pb2
from canal.protocol import CanalProtocol_pb2

from utils import createKafkaProducer

client = Client()
client.connect(host='127.0.0.1', port=11111)
client.check_valid(username=b'canals', password=b'12345678')
client.subscribe(client_id=b'1', destination=b'example', filter=b'.*\\..*')
producer = createKafkaProducer()

while True:

    message = client.get(100)
    # print(message)
    # time.sleep(5)
    entries = message['entries']
    for entry in entries:
        entry_type = entry.entryType
        if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
            # print(entry_type)
            continue
        row_change = EntryProtocol_pb2.RowChange()
        row_change.MergeFromString(entry.storeValue)
        event_type = row_change.eventType
        header = entry.header
        database = header.schemaName
        table = header.tableName
        event_type = header.eventType
        for row in row_change.rowDatas:
            format_data = dict()
            format_data_2 = dict()
            event_type_string = ''
            if event_type == EntryProtocol_pb2.EventType.DELETE:
                for column in row.beforeColumns:
                    format_data_2[column.name] =column.value
                    format_data = {
                        column.name: column.value
                    }
                format_data_2["createdAt"] = int(time.time() * 1000)
                format_data_2["operation_type"] = "DELETE"
                format_data_2['db'] = 'pythondb'
                format_data_2['table'] = 'customers'

            elif event_type == EntryProtocol_pb2.EventType.INSERT:
                format_data_2["createdAt"] = int(time.time() * 1000)
                format_data_2["operation_type"] = "INSERT"
                format_data_2['db'] = 'pythondb'
                format_data_2['table'] = 'customers'
                for column in row.afterColumns:

                    format_data_2[column.name] =column.value
                    format_data = {
                        column.name: column.value
                    }
                    # print(column)
            else:
                format_data_2["createdAt"] = int(time.time() * 1000)
                format_data_2["operation_type"] = "UPDATE"
                format_data_2['db'] = 'pythondb'
                format_data_2['table'] = 'customers'

                format_data['before'] = format_data['after'] = dict()
                for column in row.beforeColumns:
                    format_data['before'][column.name] = column.value
                for column in row.afterColumns:
                    format_data['after'][column.name] = column.value
                    format_data_2[column.name] = column.value

            data = dict(
                db=database,
                table=table,
                event_type=event_type,
                data=format_data,
                action=event_type_string,
                format_data_2=format_data_2
            )

            producer.send("dataSyncMysqlClickhouseTest2",format_data_2)

            print(data)
    # time.sleep(1)

client.disconnect()