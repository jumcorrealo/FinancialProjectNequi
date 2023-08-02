import configparser
import psycopg2
import socket
import sys
import pandas as pd

config = configparser.ConfigParser()
config.read('../config.ini')

# Get the database credentials
db_endpoint_RDS = config['database']['host']
db_name_RDS = config['database']['database_name']
db_user_RDS = config['database']['username']
db_password_RDS = config['database']['password']
db_port_RDS = int(config['database']['port'])

db_endpoint_RSH = config['redshift']['host']
db_name_RSH = config['redshift']['database_name']
db_user_RSH = config['redshift']['username']
db_password_RSH = config['redshift']['password']
db_port_RSH = int(config['redshift']['port'])

# Set the timeout for the connection attempt (in seconds)
connection_timeout = 10

try:    
    # Now establish the database connection using psycopg2
    conn_rds = psycopg2.connect(
        host=db_endpoint_RDS,
        port = db_port_RDS,
        database = db_name_RDS,
        user=db_user_RDS,
        password=db_password_RDS
    )
    print("Connected to RDS successfully!")


    
    cur_rds = conn_rds.cursor()

    # Obtener los símbolos únicos de tbTradingHistoric
    cur_rds.execute("SELECT DISTINCT \"Symbol\" FROM tbTradingHistoric;")
    symbols_tbTradingHistoric = cur_rds.fetchall()


    conn_rsh = psycopg2.connect(
        host=db_endpoint_RSH,
        port = db_port_RSH,
        database = db_name_RSH,
        user=db_user_RSH,
        password=db_password_RSH
    )
    print("Connected to RedShift successfully!")



    create_table_query = """
                INSERT INTO dimSymbol (symbol)
                SELECT DISTINCT "Symbol" FROM tbTradingHistoric th
                WHERE NOT EXISTS (
                    SELECT 1 FROM dimSymbol ds
                    WHERE ds.symbol = th."Symbol"
                );
            """


except (socket.timeout, psycopg2.OperationalError) as e:
    if isinstance(e, socket.timeout):
        print("Error: Connection timed out.")
    else:
        print("Error during connection:", e)
    sys.exit(1)  # Terminate the program with a non-zero exit code

with conn_rds.cursor() as cursor:
    cursor.execute("SELECT * FROM tbTradingHistoric;")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df_trading_historic = pd.DataFrame(data, columns=columns)