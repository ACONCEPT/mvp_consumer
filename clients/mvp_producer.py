#! usr/bin/env python3
import sys
sys.path.append("../base/")
import psycopg2
#from single_producer import producer
#from producer_menu import prompt_message, producer_menu
from kafka import KafkaProducer
from postgres_cursor import  get_cursor, execute_query,close_cursor

def get_mvp_data():
    get_cursor()
    query = "select * from parts limit 1;"
    data = execute_query(query)
    close_cursor()
    return data

def send_to_test_topic():
    data = get_mvp_data()
    producer = KafkaProducer(bootstrap_servers=['54.218.31.15:1234'])
    print(type(producer))

if __name__ == '__main__':
    print(get_mvp_data())
    send_to_test_topic()

#    producer_menu(producer,topic = "demo")