import configparser
import psycopg2
import socket
import sys
import traceback
import logging
import pandas as pd
import tqdm


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
        cur_rds.execute("SELECT * FROM tbTradingHistoric LIMIT 100000;")
        columns = [desc[0] for desc in cur_rds.description]
        data = cur_rds.fetchall()
        df_trading_historic = pd.DataFrame(data, columns=columns)
    
    
    # Obtener los símbolos únicos de tbDimSymbol
    with cur_rsh:
        cur_rsh.execute("SELECT * FROM tbDimSymbol;")
        columns = [desc[0] for desc in cur_rsh.description]
        data = cur_rsh.fetchall()
        df_DimSymbols = pd.DataFrame(data, columns=columns)
    print("Symbols fetched from tbDimSymbol successfully!")

    # Cruza la columna 'symbols' del DataFrame df_trading_historic con la columna 'Symbol' de df_DimSymbols
    merged_df = df_trading_historic.merge(df_DimSymbols, left_on='Symbol', right_on='symbol', how='left')

    # Reemplaza la columna 'symbols' por la columna 'idsymbols' del DataFrame df_DimSymbols
    merged_df['Symbol'] = merged_df['idsymbol']

    # Elimina la columna 'Symbol' que fue utilizada solo para el cruce (opcional)
    merged_df.drop(columns='symbol', inplace=True)
    merged_df.drop(columns='Symbol', inplace=True)

    # Retornar el DataFrame df_trading_historic con los ids seleccionados
    df_trading_historic_with_ids = merged_df

    return df_trading_historic_with_ids

def insert_function(df, table, conn):
    cursor = conn.cursor()
    for index, row in df.iterrows():
        insert_query = f"""INSERT INTO {table} ("idsymbol", "date", "open", "high","low","close","adj_close","volume") 
        VALUES ({row['idsymbol']},'{row['Date']}',{row['Open']},{row['High']},{row['Low']},{row['Close']},{row['Adj_Close']},{row['Volume']});
        """
        cursor.execute(insert_query)
    
    conn.commit()



try:

    # Funcion para traer lista con symbol unica
    combined_symbols = get_unique_symbols(cur_rds, cur_rsh)

    # Dividir el DataFrame en grupos de 10,000 registros
    chunk_size = 10000
    num_chunks = (len(combined_symbols) - 1) // chunk_size + 1

    for i in tqdm(range(num_chunks)):
        chunk = combined_symbols.iloc[i * chunk_size:(i + 1) * chunk_size]
        insert_function(chunk, 'tbTradingHistoric', conn_rsh)
        print(f"insert num:{num_chunks}")

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