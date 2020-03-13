import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function execute the COPY statements to extract data from S3
    and store in staging tables in Redshift
    Input: 
    - conn: connection to the database
    - cur: cursor to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function execute the INSERT statements to extract relevant data from 
    Redshift staging tables and store in dimension tables
    Input: 
    - conn: connection to the database
    - cur: cursor to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function configures connection to S3 and Redshift cluster,
    call above-defined functions to execute the ETL pipeline.
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