import configparser
import psycopg2
import sys
import socket
import traceback
import logging
import csv
import pandas as pd
import yfinance as yf
from tqdm import tqdm
import re
import datetime
import random


def save_data_to_database(data, connection):
    try:
        with connection.cursor() as cursor:
            for index, row in data.iterrows():
                date = row['Date'].strftime('%Y-%m-%d')
                open_value = row['Open']
                high = row['High']
                low = row['Low']
                close = row['Close']
                adj_close = row['Adj Close']
                volume = int(row['Volume'])
                symbol = row['Symbol']

                insert_query = f"INSERT INTO tbTradingHistoric (\"Date\", \"Open\", \"High\", \"Low\", \"Close\", \"Adj_Close\", \"Volume\", \"Symbol\") " \
                               f"VALUES ('{date}', {open_value}, {high}, {low}, {close}, {adj_close}, {volume}, '{symbol}')"

                cursor.execute(insert_query)
        
        connection.commit()
        print("¡Datos guardados exitosamente en la base de datos!")

    except psycopg2.Error as e:
        error_message = f"Error while saving data to database: {e}"
        logging.error(traceback.format_exc())
        print(error_message)

# Función para obtener datos históricos con manejo de errores
def get_historical_data(tickers, start_date, end_date):
    all_data = pd.DataFrame()

    for ticker in tqdm(tickers, bar_format='{l_bar}{bar:30}{r_bar}{bar:-30b}'):
        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            data['Symbol'] = ticker
            all_data = pd.concat([all_data, data])
        except Exception as e:
            logging.error(f"Failed download for {ticker}: {str(e)}")

    return all_data


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# loggin config
logging.basicConfig(filename='./logimports/import.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console = logging.StreamHandler()

# Read the configuration file
config = configparser.ConfigParser()
config.read('../config.ini')
console = logging.StreamHandler()
# Get the database credentials
db_endpoint = config['database']['host']
db_name = config['database']['database_name']
db_user = config['database']['username']
db_password = config['database']['password']
db_port = config['database']['port']


# Get of tickers uploaded
try:
    connection = psycopg2.connect(
        host=db_endpoint,
        database=db_name,
        user=db_user,
        password=db_password
    )

    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT \"Symbol\" FROM tbTradingHistoric;")
        existing_symbols = {row[0] for row in cursor.fetchall()}

except psycopg2.OperationalError as e:
    error_message = f"Error connecting to the database: {e}"
    logging.error(traceback.format_exc())
    print(error_message)
    sys.exit(1)

finally:
    connection.close()


# Traer del archivo logg los delisted symbols

delisted_symbols = []
max_retries_symbols = []

with open('./logimports/import.log', 'r') as log_file:
    log_content = log_file.read()

    # Use a regex pattern to find the whole message that contains 'Max retries exceeded with url'
    max_retries_matches = re.findall(r"\d{2}-\w{3}-\d{2} \d{2}:\d{2}:\d{2} - Failed to get ticker '(.+?)' reason: HTTPSConnectionPool\(host='.+?'", log_content)
    max_retries_symbols = set(max_retries_matches)

    
with open('./logimports/import.log', 'r') as log_file: 
    log_lines = log_file.readlines()
    for i in range(len(log_lines)):
            if 'Failed download' in log_lines[i]:
                match = re.search(r"\['(.+?)'\]: Exception\('.+? symbol may be delisted'\)", log_lines[i+1])
                if match:
                    symbol = match.group(1)
                    delisted_symbols.append(symbol)
delisted_symbols = [symbol for symbol in delisted_symbols if symbol not in max_retries_symbols]

# Leer el archivo CSV y obtener los tickers originales sin los delisted_symbols
original_tickers_list = []

            
with open('../resources/tickers.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Ignorar la primera fila de encabezados
    for row in reader:
        ticker = row[0]
        if ticker not in delisted_symbols and ticker not in existing_symbols:
            original_tickers_list.append(ticker)


# Obtener las fechas de inicio y fin (desde el primero de enero hasta la fecha actual)
start_date = '2023-01-01'
end_date = datetime.datetime.today().strftime('%Y-%m-%d')

# Muestra
# Dividir la lista en segmentos de 1000 tickers
segment_size = 1000
num_segments = (len(original_tickers_list) - 1) // segment_size + 1

for i in range(num_segments):
    tickers_segment = original_tickers_list[i * segment_size: (i + 1) * segment_size]

    # Obtener los datos históricos para el segmento actual
    historical_data_segment = get_historical_data(tickers_segment, start_date, end_date)
    historical_data_reset_segment = historical_data_segment.reset_index()

    # Guardar los datos en la base de datos
    try:
        connection = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=db_user,
            password=db_password
        )

        save_data_to_database(historical_data_reset_segment, connection)

    except psycopg2.OperationalError as e:
        error_message = f"Error connecting to the database: {e}"
        logging.error(traceback.format_exc())
        print(error_message)
        sys.exit(1)

    finally:
        connection.close()
