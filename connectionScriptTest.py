import configparser
import psycopg2
import sys
import socket

# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the database credentials
db_endpoint = config['database']['host']
db_name = config['database']['database_name']
db_user = config['database']['username']
db_password = config['database']['password']
# Obtener el valor del puerto del archivo de configuración
db_port_str = config['database']['port']

# Eliminar comillas dobles y espacios en blanco alrededor del valor del puerto
db_port = int(db_port_str.strip().replace('"', ''))

# Set the timeout for the connection attempt (in seconds)
connection_timeout = 60

try:
    # Create a socket and set a timeout for the connection attempt
    conn_socket = socket.create_connection(('172.31.96.93', 5432), timeout=connection_timeout)
    
    # If the connection was successful, close the socket
    conn_socket.close()
    
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
