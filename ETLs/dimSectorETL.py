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


def get_unique_symbols(cur_rds, cur_rsh):

    cur_rds.execute("SELECT DISTINCT \"Sector\",\"Industry\" FROM tbtickers;")
    sectorIndustry_tbtickers = cur_rds.fetchall()
    print("Symbols fetched from tbtickers successfully!")
    

    # Obtener los Sector,Industry únicos de tbdimsector
    cur_rsh.execute("SELECT DISTINCT \"sector\",\"industry\" FROM tbdimsector;")
    sectorIndustry_tbdimsector = cur_rsh.fetchall()
    print("Symbols fetched from tbdimsector successfully!")

    # Reemplazar valores vacíos en sectorIndustry_tbtickers por 'N/A'
    sectorIndustry_tbtickers = [(sector if sector else 'N/A', industry if industry else 'N/A') for sector, industry in sectorIndustry_tbtickers]
    
    # Lista de comprensión para eliminar los símbolos existentes de sectorIndustry_tbtickers
    unique_symbols = [(sector, industry) for sector, industry in sectorIndustry_tbtickers if (sector, industry) not in sectorIndustry_tbdimsector]
    
    return unique_symbols


try:

    # Funcion para traer lista con Sector unica
    sectorIndustry_tbtickers = get_unique_symbols(cur_rds, cur_rsh)
    
    # Consulta de inserción
    insert_query = "INSERT INTO tbdimsector (Sector,Industry) VALUES (%s, %s)"

    with conn_rsh.cursor() as cur_rsh:
        for Sector, Industry in sectorIndustry_tbtickers:
            try:
                cur_rsh.execute(insert_query, (Sector,Industry))
                
            except psycopg2.Error as e:
                print(f"Error occurred during insertion for Sector '{Sector}':", e)
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