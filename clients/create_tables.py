#! usr/bin/env python3
import psycopg2
import sys
import os

def get_conn_string():
    try:
        with open("../postgres_connection_string","r") as f:
            return f.read().strip()
    except:
        return os.environ.get("PYTHON_POSTGRES_CONN")

"""
DROP TABLE IF EXISTS part_customers;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS part_suppliers;
DROP TABLE IF EXISTS suppliers;
DROP TABLE IF EXISTS sales_orders;
DROP TABLE IF EXISTS purchase_orders;
DROP TABLE IF EXISTS parts;
DROP TABLE IF EXISTS sites;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS inventory_movements;
DROP TABLE IF EXISTS work_orders;
"""

def create_tables(conn_string):
    """ create tables in the PostgreSQL database"""
    commands = (
    """
    DROP TABLE IF EXISTS transformed_parts;
    """,
    """
    CREATE TABLE IF NOT EXISTS transformed_parts (
        id INTEGER PRIMARY KEY,
        attrs JSON)
    """)
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception) as error:
        print(error)
        raise error
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    conn_string = get_conn_string()
    create_tables(conn_string)

