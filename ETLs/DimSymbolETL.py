import configparser
import psycopg2
import socket
import sys
import traceback
import logging



for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# loggin config
logging.basicConfig(filename='./logETLs/scripts.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console = logging.StreamHandler()


config = configparser.ConfigParser()
config.read('../config.ini')
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

def connect_to_RDS():
    conn_rds = psycopg2.connect(
            host=db_endpoint_RDS,
            port = db_port_RDS,
            database = db_name_RDS,
            user=db_user_RDS,
            password=db_password_RDS
        )
    return conn_rds

def connection_to_RedShift():

    conn_rsh = psycopg2.connect(
            host=db_endpoint_RSH,
            port = db_port_RSH,
            database = db_name_RSH,
            user=db_user_RSH,
            password=db_password_RSH
        )
    
    return conn_rsh


try:    

    conn_rds = connect_to_RDS()
    cur_rds = conn_rds.cursor()
    conn_rsh = connection_to_RedShift()
    cur_rsh = conn_rsh.cursor()


except (socket.timeout, psycopg2.OperationalError) as e:
    if isinstance(e, socket.timeout):
        print("Error: Connection timed out.")
    else:
        print("Error during connection:", e)
        logging.error(traceback.format_exc())
    sys.exit(1)  # Terminate the program with a non-zero exit code


try:
    # Obtener los símbolos únicos de tbTradingHistoric
    cur_rds.execute("SELECT DISTINCT \"Symbol\" FROM tbTradingHistoric;")
    symbols_tbTradingHistoric = cur_rds.fetchall()
    print("Symbols fetched from tbTradingHistoric successfully!")

    # Obtener los símbolos únicos de tbDimSymbol
    cur_rsh.execute("SELECT DISTINCT \"Symbol\" FROM tbDimSymbol;")
    symbols_tbdimSymbols = cur_rsh.fetchall()
    print("Symbols fetched from tbDimSymbol successfully!")

    # Lista de comprensión para eliminar los símbolos existentes de symbols_tbTradingHistoric
    symbols_tbTradingHistoric = [symbol for symbol in symbols_tbTradingHistoric if symbol not in symbols_tbdimSymbols]

    # Consulta de inserción
    insert_query = "INSERT INTO tbDimSymbol (Symbol) VALUES (%s)"

    with conn_rsh.cursor() as cur_rsh:
        for symbol in symbols_tbTradingHistoric:
            try:
                cur_rsh.execute(insert_query, symbol)
                
            except psycopg2.Error as e:
                print(f"Error occurred during insertion for symbol '{symbol[0]}':", e)
                logging.error(traceback.format_exc())
                conn_rsh.rollback()  # Rollback the transaction in case of an error

    conn_rsh.commit()  # Commit all the successful insertions

except (socket.timeout, psycopg2.OperationalError) as e:
    if isinstance(e, socket.timeout):
        print("Error: Connection timed out.")
    else:
        print("Error during connection:", e)
        logging.error(traceback.format_exc())
    sys.exit(1)  # Terminate the program with a non-zero exit code
except psycopg2.Error as e:
    print("Error occurred during SQL query:", e)
    logging.error(traceback.format_exc())
    conn_rsh.rollback()  # Rollback the transaction in case of an error


conn_rds.close()
conn_rsh.close()