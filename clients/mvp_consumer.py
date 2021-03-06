import sys
from kafka import KafkaConsumer
import os
from postgres_cursor import  get_cursor, execute_cursor,close_cursor,commit_connection
from queries import get_base_table_descriptions
import json

def read_table_definitions():
    with open("base_table_definitions.json","r") as f:
        data = json.loads(f.read())
    return data

def generate_insert_statement(**kwargs):
    global DEFINITIONS
    base_stmt = "insert into {} ({}) values ({});"
    table = kwargs.pop("table")
    column_definitions = DEFINITIONS.get(table)
    cols = []
    vals = []
    for column,data in kwargs.items():
        column_type = column_definitions.get(column,False)
        if not column_type:
            print(column_definitions)
        else:
            cols.append(column)
            if "CHAR" in column_type.upper() or "JSON" in column_type.upper():
                val = "'{}'".format(data)
            elif "INT" in column_type.upper():
                val = "{}".format(str(int(data)))
            elif "TIME" in column_type.upper():
                val = "TIMESTAMP '{}'".format(data.isoformat())
            else:
                val  = data
            vals.append(val)
    columns = ", ".join(cols)
    values = ", " .join(vals)
    stmt = base_stmt.format(table,columns, values)
    return stmt


def consume_test_topic(bootstrap_servers):
    insert_args = {}
    consumer = KafkaConsumer("test",\
            group_id  = "test",\
            bootstrap_servers=bootstrap_servers,\
            auto_offset_reset ="smallest",\
            value_deserializer =lambda m: json.loads(m.decode('utf-8')))
    base_insert = "insert into {} ({}) values ({});"
    for message in consumer:
        val =json.loads( message.value)
        try:
            get_cursor()
            val = json.loads(message.value)
            table = val.pop("table")
            insert_args["table"]="transformed_{}".format(table)
            insert_args["id"]=val.pop("id")
            insert_args["attrs"] = json.dumps(val)
            print("=" * 50)
            print(" " * 15 + "TRANSFORMED SQL RECORD")
            print("ORIGINAL TABLE {}, NEW TABLE {}".format(table,insert_args["table"]))
            for key, item in insert_args.items():
                  print("COLUMN : {}".format(key))
                  print("VALUE : {}".format(item))
            insert_statement = generate_insert_statement(**insert_args)
            execute_cursor(insert_statement)
            commit_connection()
            close_cursor()
        except Exception as e:
            print("exception {} on offset {}".format(e,message.offset))

if __name__ == '__main__':
    global DEFINITIONS
    fn = os.environ.get("HOME") +"/clusterinfo"
    with open(fn ,"r") as f:
        bootstrap_servers = ["{}:9092".format(x) for x in f.readlines()]
    get_base_table_descriptions()
    DEFINITIONS = read_table_definitions()
    consume_test_topic(bootstrap_servers)

