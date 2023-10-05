
from flask import Flask, request
import psycopg2
from dotenv import load_dotenv
import os
import logging

load_dotenv()
connection_string = os.getenv("EXTERNAL_DATABASE_URL")
connection = None
connection = psycopg2.connect(connection_string)

cur = connection.cursor()

# query = 'SELECT * FROM books LIMIT %s OFFSET %s;'
# params = (10, 0)
def custom_query(query, params):
    cur.execute(query, params)
    data = cur.fetchall()
    print(data)
    return data


# Commit the changes to the database
connection.commit()

# custom_query(query=query, params=params)

# Close the cursor and connection
cur.close()
connection.close()

print("Connection Closed Successfully")

