import configparser
import psycopg2
import sys
import traceback
import logging

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# loggin config
logging.basicConfig(filename='./logScripts/scripts.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')


console = logging.StreamHandler()

# Read the configuration file
config = configparser.ConfigParser()
config.read('../config.ini')

# Get the database credentials
db_endpoint = config['redshift']['host']
db_name = config['redshift']['database_name']
db_user = config['redshift']['username']
db_password = config['redshift']['password']
db_port = 5439

# Set the timeout for the connection attempt (in seconds)
connection_timeout = 10

try:

    # Now establish the database connection using psycopg2
    connection = psycopg2.connect(
        host=db_endpoint,
        database=db_name,
        user=db_user,
        password=db_password
    )
    print("¡Conexión exitosa a la base de datos!")
    # Create a table if it doesn't exist

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS tbDimSymbol (
        "idSymbol" SERIAL PRIMARY KEY,
        "Symbol" VARCHAR(200),
    );
    '''

    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
        connection.commit()
        print("¡Tabla creada exitosamente!")

    except psycopg2.Error as e:
        error_message = f"Error creating table: {e}"
        logging.error(traceback.format_exc())
        sys.exit(1)  # Terminar el programa con un código de salida no cero

    finally:
        connection.close()

except (psycopg2.OperationalError) as e:
    if isinstance(e):
        error_message = "Error: Connection timed out."
    else:
        error_message = "Error during connection: {e}"
        logging.error(traceback.format_exc())

    logging.error(error_message)
    sys.exit(1)  # Terminate the program with a non-zero exit code

