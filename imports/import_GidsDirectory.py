import configparser
import psycopg2
import sys
import traceback
import logging
import pandas as pd

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# logging config
logging.basicConfig(filename='./logimports/import.log', level=logging.ERROR,
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

    # Read the data from the Excel file
    excel_file_path = '../resources/GIDS_Directory_20230731.xlsx'
    df = pd.read_excel(excel_file_path)

    with connection.cursor() as cursor:
        # Iterate through the data and insert it into the table
        for index, row in df.iterrows():
            symbol = row['Symbol']
            name = row['Name'].replace("'", "''")
            type = row['Type']

            # Create the SQL statement to insert the data into the table
            insert_query = f'''
                INSERT INTO tbgidsdirectory ("Symbol", "Name", "Type")
                VALUES ('{symbol}', '{name}', '{type}');
            '''

            # Execute the insertion query
            cursor.execute(insert_query)

    # Commit the changes to the database
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
    sys.exit(1)  # Terminate the program with a non-zero exit code

finally:
    connection.close()
