from clickhouseConsumer import clickhouseConsumer
from moveData import moveData
from utils import getConfig, getMysqlSchema


def mainDataSynchronization(mysqlDB, mysqlTable, clickhouseDB, 
    clickhouseTable, kafkaTopic, KafkaGroup):

    # connecting to mysql database
    db = getConfig(mysqlDB)

    # creating clickhouse database and table if not exist and connecting
    # clickhouse to kafka
    mysqlSchema  = getMysqlSchema(db, mysqlTable)
    isClickhouseTableCreated = clickhouseConsumer(clickhouseDB, clickhouseTable, mysqlSchema,
        kafkaTopic, KafkaGroup)
    
    if(not isClickhouseTableCreated):
        return

    # moving initail data in mysql to clickhouse
    moveData(db, mysqlTable, kafkaTopic) 

    
    

if __name__ == "__main__":
    mainDataSynchronization("pythondb", "customers", "clickhouseTest",
        "customers14","dataSyncMysqlClickhouseTest2","group7")

