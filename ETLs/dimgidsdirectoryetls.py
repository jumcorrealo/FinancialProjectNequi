import configparser
import psycopg2
import socket
import sys
import traceback
import logging
import pandas as pd
import re

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
    # Obtener los símbolos únicos de tbTradingHistoric
    with cur_rds:
        cur_rds.execute("SELECT * FROM tbGIDSDirectory;")
        columns = [desc[0] for desc in cur_rds.description]
        data = cur_rds.fetchall()
        df_gidsdirectory = pd.DataFrame(data, columns=columns)
    
    
    # Obtener los símbolos únicos de tbDimSymbol
    with cur_rsh:
        cur_rsh.execute("SELECT * FROM tbDimSymbol;")
        columns = [desc[0] for desc in cur_rsh.description]
        data = cur_rsh.fetchall()
        df_DimSymbols = pd.DataFrame(data, columns=columns)
    print("Symbols fetched from tbDimSymbol successfully!")

    # Cruza la columna 'symbols' del DataFrame df_gidsdirectory con la columna 'Symbol' de df_DimSymbols
    merged_df = df_gidsdirectory.merge(df_DimSymbols, left_on='Symbol', right_on='symbol', how='left')

    # Reemplaza la columna 'symbols' por la columna 'idsymbols' del DataFrame df_DimSymbols
    merged_df['Symbol'] = merged_df['idsymbol']

    # Elimina la columna 'Symbol' que fue utilizada solo para el cruce (opcional)
    merged_df.drop(columns='symbol', inplace=True)
    merged_df.drop(columns='Symbol', inplace=True)

    # Retornar el DataFrame df_gidsdirectory con los ids seleccionados
    df_gidsdirectory_with_ids = merged_df

    return df_gidsdirectory_with_ids

def insert_function(df, table, conn):
    cursor = conn.cursor()
    for index, row in df.iterrows():
        insert_query = f"""INSERT INTO {table} (\"idsymbol\", \"name\", \"type\") 
        VALUES ('{row['idsymbol']}',{row['Name']},{row['Type']});
        """
        cursor.execute(insert_query)
    
    conn.commit()
    
# Función para agrupar nombres similares
def group_names(name):
    # Expresión regular para buscar patrones en el nombre
    pattern = r'(\bNasdaq-100\b)|(\bNasdaq\b)|(\b[A-Z]+ ETF\b)|(\b[A-Z]+ Muni Bond ETF\b)' +\
            r'|(\bS&P\b)|(\bNASDAQ\b)|(\bThe Capital Strength\b)|(\bSettle\b.*)|(\bFidelity Disruptive\b.*)' + \
            r'|(\bPHLX\b)|(\bThe Capital Strength\b)|(\bOMX\b)|(\bOMRX\b)|(\bFirst North\b)'+\
            r'|(\bDorsey Wright\b)|(\bCompass EMP\b)|(\bGlobal X\b)|(\bOptimal Blue 30Yr\b)'+\
            r'|(\bStrategic Technology & Ecommerce\b)|(\Strategic Hotel & Lodging\b)|(\bStrategic E-Commerce\b)'+\
            r'|(\bStrategic Fintech & Digital Payments\b)|(\bCRSP US\b)'

    match = re.search(pattern, name)
    if match:
        return match.group()
    return name


try:

    # Funcion para traer lista con symbol unica
    combined_symbols = get_unique_symbols(cur_rds, cur_rsh)
    # Aplicar la función a la columna 'Name' para obtener la nueva columna 'Grouped Name'
    combined_symbols['Name'] = combined_symbols['Name'].apply(group_names)
    insert_function(combined_symbols,'tbdimgidsdirectory',conn_rsh)


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