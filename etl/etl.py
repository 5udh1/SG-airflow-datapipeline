import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Execute SQL queries to load data from S3 into staging tables on Redshift.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Execute SQL queries to transform data and insert it into the analytics tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Connect to Redshift and orchestrate the ETL process.
    """
    # Load configuration from dwh.cfg
    config = configparser.ConfigParser()
    config.read('config/dwh.cfg')

    # Establish connection to Redshift
    conn = psycopg2.connect(
        host=config.get('DWH', 'DWH_ENDPOINT'),
        dbname=config.get('DWH', 'DWH_DB'),
        user=config.get('DWH', 'DWH_DB_USER'),
        password=config.get('DWH', 'DWH_DB_PASSWORD'),
        port=config.get('DWH', 'DWH_PORT')
    )
    cur = conn.cursor()
    
    # Execute ETL steps
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close connection
    conn.close()

if __name__ == "__main__":
    main()