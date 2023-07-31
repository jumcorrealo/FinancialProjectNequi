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
db_endpoint = config['database']['host']
db_name = config['database']['database_name']
db_user = config['database']['username']
db_password = config['database']['password']
db_port = config['database']['port']

try:

    # Now establish the database connection using psycopg2
    connection = psycopg2.connect(
        host=db_endpoint,
        database=db_name,
        user=db_user,
        password=db_password
    )
    print("¡Conexión exitosa a la base de datos!")

    # Leer el archivo de datos y dividir las líneas
    with open('../resources/tickers.csv', 'r') as file:
        lines = file.readlines()

    with connection.cursor() as cursor:
        # Recorrer las líneas del archivo omitiendo la primera línea que contiene los nombres de columna
        for line in lines[1:]:
            data = line.strip().split(',')
            symbol = data[0]
            name = data[1]
            country = data[6]
            ipo_year = int(data[7])
            volume = int(data[8])
            sector = data[9]
            industry = data[10]

            # Crear la sentencia SQL para insertar los datos en la tabla
            insert_query = f'''
            INSERT INTO tbtickers ("Symbol", "Name", "Country", "IPO_Year", "Volome", "Sector", "Industry")
            VALUES ('{symbol}', '{name}', '{country}', {ipo_year}, {volume}, '{sector}', '{industry}');
            '''

            # Ejecutar la sentencia de inserción
            cursor.execute(insert_query)

    # Confirmar los cambios en la base de datos
    connection.commit()
    print("Datos insertados exitosamente en la tabla.")

except (psycopg2.OperationalError) as e:
    if isinstance(e, psycopg2.OperationalError):
        error_message = "Error: Connection timed out."
    else:
        error_message = f"Error durante la conexión: {e}"
        logging.error(traceback.format_exc())

    logging.error(error_message)
    sys.exit(1)  # Terminate the program with a non-zero exit code

except psycopg2.Error as e:
    error_message = f"Error al interactuar con la base de datos: {e}"
    logging.error(traceback.format_exc())
    sys.exit(1)  # Terminar el programa con un código de salida no cero

finally:
    connection.close()
