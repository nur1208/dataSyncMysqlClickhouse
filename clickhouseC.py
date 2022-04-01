from clickhouse_driver import Client

client = Client(host='localhost',user="default",password="nur")

result = client.execute('SHOW DATABASES')
# result = client.execute("SELECT name, comment FROM system.databases WHERE name = 'db_comment';")
# result = client.execute("CREATE DATABASE pythondb ENGINE = Memory COMMENT 'test data sync';")


# sql="ALTER TABLE `%s`.`%s` DELETE WHERE id = %s ;" %("pythondb" ,
#         binlogevent.table,binlogevent.primary_key,
#         info[binlogevent.primary_key])
print(result)