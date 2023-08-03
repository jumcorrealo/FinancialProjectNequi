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

    cur_rds.execute("SELECT DISTINCT \"Country\" FROM tbtickers;")
    Country_tbtickers = cur_rds.fetchall()
    print("Symbols fetched from tbtickers successfully!")
    

    # Obtener los Country únicos de tbdimcountry
    cur_rsh.execute("SELECT DISTINCT \"country\" FROM tbdimcountry;")
    country_tbcountry = cur_rsh.fetchall()
    print("Symbols fetched from tbdimcountry successfully!")

    Country_tbtickers['Country'] = Country_tbtickers['Country'].repacle('','N/A')
    # Lista de comprensión para eliminar los símbolos existentes de Country_tbtickers
    unique_symbols = list(set([country[0] for country in Country_tbtickers if country not in country_tbcountry]))
    
    return unique_symbols


try:

    # Funcion para traer lista con country unica
    Country_tbtickers = get_unique_symbols(cur_rds, cur_rsh)
    
    # Consulta de inserción
    insert_query = "INSERT INTO tbdimcountry (Country) VALUES (%s)"

    with conn_rsh.cursor() as cur_rsh:
        for country in Country_tbtickers:
            try:
                cur_rsh.execute(insert_query, (country,))
                
            except psycopg2.Error as e:
                print(f"Error occurred during insertion for country '{country[0]}':", e)
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