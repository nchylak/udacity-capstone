import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.config')

    conn = psycopg2.connect(f"\
        host={config['DWH']['HOST']} \
        dbname={config['DWH']['DWH_DB_NAME']} \
        user={config['DWH']['DWH_DB_USER']} \
        password={config['DWH']['DWH_DB_PASSWORD']} \
        port={config['DWH']['DWH_PORT']}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()