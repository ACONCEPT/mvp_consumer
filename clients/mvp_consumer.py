import sys
sys.path.append("../base/")
#import psycopg2
from kafka import KafkaConsumer
from postgres_cursor import  get_cursor, execute_query,close_cursor
import json

def consume_test_topic():
    consumer = KafkaConsumer("test",\
            group_id  = "test",\
            bootstrap_servers=['54.218.31.15:9092'],\
            auto_offset_reset ="smallest",\
            value_deserializer =lambda m: json.loads(m.decode('ascii')))
    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                  message.offset, message.key,
                                                  message.value))

if __name__ == '__main__':
    consume_test_topic()
