import json
import sys
import psycopg2
import boto3
import yaml
import datetime
from psycopg2.extras import LogicalReplicationConnection

with open("config.yml", "r") as stream:
    try:
        config = yaml.safe_load(stream=stream)
    except yaml.YAMLError as yamlErr:
        print(yamlErr)

# SETUP CONFIGURATION
dbConfig = config['instances']['pg']
repluserConfig = config['pg2kinesis']['repluser']
kinesisConfig = config['pg2kinesis']['kinesis']

stream_name = kinesisConfig['stream_name']
kinesis_client = boto3.client(
    'kinesis',
    aws_access_key_id=kinesisConfig['credentials']['aws_access_key_id'],
    aws_secret_access_key=kinesisConfig['credentials']['aws_secret_access_key'],
    region_name=kinesisConfig['region_name'])

class Consumer:
    def __call__(self, msg):
        jsonPayload = json.loads(msg.payload)['change']
        if not jsonPayload:
            print("Nothing changed, skip this message!\n")
            pass
        else:
            for changeData in jsonPayload:
                currentTime = datetime.datetime.now()
                if changeData['kind'] == 'delete':
                    columnnames = changeData['oldkeys']['keynames']
                    columnvalues = changeData['oldkeys']['keyvalues']
                else:
                    columnnames = changeData['columnnames']
                    columnvalues = changeData['columnvalues']
                processedData = {k : v for k, v in zip(columnnames, columnvalues)}
                processedData['yr'] = processedData.pop('year')
                processedData['mth'] = processedData.pop('month')
                processedData["event_timestamp"] = currentTime
                processedData['kind'] = changeData['kind']
                processedData['table'] = changeData['table']
                print(f"Data: {processedData}")
                print("Put one record to Kinesis...\n")
                kinesis_client.put_record(StreamName=stream_name, Data=json.dumps(processedData), PartitionKey="default")

def main():
    global dbConfig, repluserConfig
    # ESTABLISH SSH CONNECTION
    try:
        my_connection  = psycopg2.connect(
                        f"dbname='{dbConfig['dbname']}' host='{dbConfig['host']}' port=5432 user='{repluserConfig['username']}' password='{repluserConfig['password']}'" ,
                        connection_factory = LogicalReplicationConnection)
    except Exception as e:
        print("ERROR: Unexpected error: Could not connect to RDS for PostgreSQL instance.")
        print(e)
        sys.exit()

    print("SUCCESS: Connection to RDS for PostgreSQL instance succeeded")
    cur = my_connection.cursor()
    options = {'pretty-print' : 1}

    # CREATE REPLICATION SLOTS
    try:
        cur.create_replication_slot('cdc', output_plugin = 'wal2json')

    # IN CASE THE REPLICATION SLOT EXISTS, DROP AND CREATE NEW ONE
    except psycopg2.ProgrammingError:
        print("Replication slot exists!")
        cur.drop_replication_slot(slot_name='cdc')
        cur.create_replication_slot(slot_name='cdc', output_plugin = 'wal2json')

    # START LISTENING TO CHANGE DATA
    cur.start_replication(slot_name='cdc', options=options, decode= True)
    consumer = Consumer()
    cur.consume_stream(consume=consumer)

if __name__ == "__main__":
    main()