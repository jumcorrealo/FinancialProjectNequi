import configparser
import psycopg2
import socket
import sys
import traceback
import logging
import pandas as pd



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
    # Obtener los símbolos únicos de tbTickers
    with cur_rds:
        cur_rds.execute("SELECT * FROM tbTickers;")
        columns = [desc[0] for desc in cur_rds.description]
        data = cur_rds.fetchall()
        df_trading_historic = pd.DataFrame(data, columns=columns)
    
    
    # Obtener los símbolos únicos de tbDimSymbol
    with cur_rsh:
        cur_rsh.execute("SELECT * FROM tbDimSymbol;")
        columns = [desc[0] for desc in cur_rsh.description]
        data = cur_rsh.fetchall()
        df_DimSymbols = pd.DataFrame(data, columns=columns)
    

        cur_rsh.execute("SELECT * FROM tbDimcountry;")
        columns = [desc[0] for desc in cur_rsh.description]
        data = cur_rsh.fetchall()
        df_Dimcountrys = pd.DataFrame(data, columns=columns)
    
   
        cur_rsh.execute("SELECT * FROM tbDimsector;")
        columns = [desc[0] for desc in cur_rsh.description]
        data = cur_rsh.fetchall()
        df_Dimsectors = pd.DataFrame(data, columns=columns)
    print("Symbols fetched from tbDimSymbol successfully!")
    print("countrys fetched from tbDimcountry successfully!")
    print("sectors fetched from tbDimsector successfully!")




    # Cruza la columna 'symbols' del DataFrame df_trading_historic con la columna 'Symbol' de df_DimSymbols
    merged_df = df_trading_historic.merge(df_DimSymbols, left_on='Symbol', right_on='symbol', how='left')

    # Cruza la columna 'country' del DataFrame df_trading_historic con la columna 'Country' de df_Dimcountrys
    merged_df = merged_df.merge(df_Dimcountrys, left_on='Country', right_on='country', how='left')

    # Cruza la columna 'sector' e 'industry' del DataFrame merged_df con las columnas 'idsector', 'Sector' e 'Industry' de df_Dimsectors
    merged_df = merged_df.merge(df_Dimsectors, left_on=['Sector', 'Industry'], right_on=['sector', 'industry'], how='left')
    # Elimina la columna 'Symbol' que fue utilizada solo para el cruce (opcional)
    merged_df.drop(columns=['symbol', 'Symbol', 'Sector', 'sector', 'Industry', 'industry', 'Country', 'country'], inplace=True)

    merged_df['IPO_Year'] = merged_df['IPO_Year'].replace('', 1900)
    merged_df['IPO_Year'] = merged_df['IPO_Year'].fillna(1900)

    return merged_df


def insert_function(df, table, conn):
    cursor = conn.cursor()
    for index, row in df.iterrows():
        insert_query = f"""INSERT INTO {table} ("name", "IPO_Year", "idsymbol", "idcountry","idsector") 
        VALUES ('{row['Name']}',{row['IPO_Year']},{row['idsymbol']},{row['idcountry']},{row['idsector']});
        """
        cursor.execute(insert_query)
    
    conn.commit()

try:

    # Funcion para traer lista con symbol unica
    combined_symbols = get_unique_symbols(cur_rds, cur_rsh)

    insert_function(combined_symbols,'tbTickers',conn_rsh)

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