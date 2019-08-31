import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables in database.

    Input:
        cur - cursor connection for db
        conn - connection for db
    Returns:
        None
    """

    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Successfully Dropping Table")
        except psycopg2.Error as e:
            print(e)
            conn.close()


def create_tables(cur, conn):
    """
    Creates all tables in database.

    Input:
        cur - cursor connection for db
        conn - connection for db
    Returns:
        None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("Successfully Creating Table")
        except psycopg2.Error as e:
            print(e)
            conn.close()


def main():
    """
    Creates all tables after verfying AWS user credentials
    from config file, and dropping tables if they already exist.

    Input:
        None
    Returns:
        None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()