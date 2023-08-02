import configparser
import psycopg2
import socket
import sys
import pandas as pd

config = configparser.ConfigParser()
config.read('../config.ini')

# Get the database credentials
db_endpoint = config['database']['host']
db_name = config['database']['database_name']
db_user = config['database']['username']
db_password = config['database']['password']
db_port = int(config['database']['port'])

# Set the timeout for the connection attempt (in seconds)
connection_timeout = 10

try:    
    # Now establish the database connection using psycopg2
    connection = psycopg2.connect(
        host=db_endpoint,
        port = db_port,
        database = db_name,
        user=db_user,
        password=db_password
    )
    print("Connected successfully!")
    

except (socket.timeout, psycopg2.OperationalError) as e:
    if isinstance(e, socket.timeout):
        print("Error: Connection timed out.")
    else:
        print("Error during connection:", e)
    sys.exit(1)  # Terminate the program with a non-zero exit code

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM tbTradingHistoric;")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df_trading_historic = pd.DataFrame(data, columns=columns)