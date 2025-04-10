import psycopg2
import configparser

# Read the configuration file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Extract connection details from config file
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_ENDPOINT = config.get('DWH', 'DWH_ENDPOINT')
DWH_PORT = config.get('DWH', 'DWH_PORT')
DWH_DB = config.get('DWH', 'DWH_DB')

# Create the connection string
conn_string = "postgresql://{}:{}@{}:{}/{}".format(
    DWH_DB_USER,
    DWH_DB_PASSWORD,
    DWH_ENDPOINT,
    DWH_PORT,
    DWH_DB
)

def connect_to_postgres():
    """
    Establishes connection to PostgreSQL using psycopg2 and prints the connection details.
    """
    try:
        # Establish connection
        conn = psycopg2.connect(
            dbname=DWH_DB,
            user=DWH_DB_USER,
            password=DWH_DB_PASSWORD,
            host=DWH_ENDPOINT,
            port=DWH_PORT
        )
        print("Connection successful!")
        print(f"Connection string: {conn_string}")
        
        # Return connection
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

if __name__ == "__main__":
    # Attempt to connect to PostgreSQL
    connection = connect_to_postgres()

    # Perform any cleanup (e.g., close connection) if needed
    if connection:
        connection.close()
        print("Connection closed.")