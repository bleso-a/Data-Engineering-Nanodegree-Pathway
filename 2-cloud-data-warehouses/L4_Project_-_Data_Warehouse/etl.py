import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, copy_table_queries, insert_table_order, copy_table_order


def load_staging_tables(cur, conn):
    """
    Load data from the logs to the staging tables
    :param cur: The cursor
    :param conn: The database connection
    :return:None
    """
    cnt = 0
    for query in copy_table_queries:
        print("Copying data into {}...".format(copy_table_order[cnt]))
        cur.execute(query)
        conn.commit()
        cnt = cnt + 1
        print("Loading Completed!")


def insert_tables(cur, conn):
    """
    Insert data from staging tables into another table for analysis
    :param cur: The cursor
    :param conn: The Database Connection
    """
    
    cnt = 0
    for query in insert_table_queries:
        print(Inserting data into {}. format(insert_table_order[cnt]))
        cur.execute(query)
        conn.commit()
        cnt += 1
        print("Loading Completed!")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()