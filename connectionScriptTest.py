import configparser
import psycopg2
import sys


# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Get the database credentials
db_endpoint = config['database']['host']
db_name = config['database']['database_name']
db_user = config['database']['username']
db_password = config['database']['password']

try:
    # Set the timeout to 10 seconds for the connection attempt
    connection = psycopg2.connect(
        host=db_endpoint,
        database=db_name,
        user=db_user,
        password=db_password,
        timeout=10  # Set the timeout in seconds
    )
    print("Connected successfully!")
except psycopg2.OperationalError as e:
    if "timeout expired" in str(e):
        print("Error: Connection timed out.")
    else:
        print("Error during connection:", e)
    sys.exit(1)  # Terminate the program with a non-zero exit code
