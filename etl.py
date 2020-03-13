import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function executes the COPY statements to extract data from S3
    and store in staging tables in Redshift
    
    Arguments: 
    - conn: connection to the database
    - cur: cursor to the database
    
    Return:
    - None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function executes the INSERT statements to extract relevant data from 
    Redshift staging tables and store in dimension tables
    
    Arguments: 
    - conn: connection to the database
    - cur: cursor to the database
    
    Return:
    - None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function configures connection to S3 and Redshift cluster,
    calling above-defined functions to execute the ETL pipeline.
    
    Arguments:
    -None
    
    Return:
    -None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()