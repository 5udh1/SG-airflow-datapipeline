import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Executes all queries to drop tables.
    :param cur: Database cursor
    :param conn: Database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables dropped successfully!")

def create_tables(cur, conn):
    """
    Executes all queries to create tables.
    :param cur: Database cursor
    :param conn: Database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Tables created successfully!")

def main():
    """
    Establishes connection, drops existing tables, and creates new ones.
    """
    # Load configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    # Drop and create tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close connection
    conn.close()
    print("Database setup completed!")

if __name__ == "__main__":
    main()