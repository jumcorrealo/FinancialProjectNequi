import configparser
import psycopg2
import sys
import socket
import traceback
import logging

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# loggin config
logging.basicConfig(filename='log/connection.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console = logging.StreamHandler()

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')
console = logging.StreamHandler()
# Get the database credentials
db_endpoint = config['database']['host']
db_name = config['database']['database_name']
db_user = config['database']['username']
db_password = config['database']['password']
db_port = config['database']['port']

# Set the timeout for the connection attempt (in seconds)
connection_timeout = 10

try:
    # Create a socket and set a timeout for the connection attempt
    conn_socket = socket.create_connection((db_endpoint, 5432), timeout=connection_timeout)

    # If the connection was successful, close the socket
    conn_socket.close()

    # Now establish the database connection using psycopg2
    connection = psycopg2.connect(
        host=db_endpoint,
        database=db_name,
        user=db_user,
        password=db_password
    )
    print("¡Conexión exitosa a la base de datos!")

except (socket.timeout, psycopg2.OperationalError) as e:
    if isinstance(e, socket.timeout):
        error_message = "Error: Connection timed out."
    else:
        error_message = "Error during connection: {e}"
        logging.error(traceback.format_exc())

    print(error_message)
    logging.error(error_message)
    sys.exit(1)  # Terminate the program with a non-zero exit code
