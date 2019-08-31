import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the staging tables for ETL process.

    Input:
        cur - cursor connection for db
        conn - connection for db
    Returns:
        None
    """

    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()


def insert_tables(cur, conn):
    """
    Insert data into each table.

    Input:
        cur - cursor connection for db
        conn - connection for db
    Returns:
        None
    """

    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()


def main():
    """
    Loads the staging tables for ETL process.

    Input:
        None
    Returns:
        None
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