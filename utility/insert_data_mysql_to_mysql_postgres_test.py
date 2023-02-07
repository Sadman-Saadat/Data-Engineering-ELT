import psycopg2
from psycopg2 import extras
import pandas as pd
from datetime import datetime
import mysql
from configuration.config import Credential
from utility.truncate_data import truncate_data

credential = Credential()


def insert_data_mysql(src_conn,
                      src_cursor,
                      tgt_conn=None,
                      tgt_cursor=None,
                      src_schema=None,
                      tgt_schema=None,
                      table_name=None):
    sql_code = f"""select * from {src_schema}.{table_name}"""  # src table
    try:
        src_cursor.execute(sql_code)  # execute query in src
        data = src_cursor.fetchall()  # get data from src
        df = pd.DataFrame(data)  # convert it to DataFrame
        cols = ",".join([col[0] for col in src_cursor.description])  # columns
        # =====================================
        # add data & column in same DataFrame
        # =====================================
        df.loc[-1] = cols.split(',')  # add column names in dataFrame
        df.index = df.index + 1  # shifting index
        df.sort_index(inplace=True)
        df.columns = df.iloc[0]  # use 1st row as header
        df = df.iloc[1:, :]  # df[1:]: 1st row as 0 index is feature name (col)
        # ===========================================
        # convert str to MySQL datetime (ms excluded)
        # ===========================================
        df['last_update'] = df['last_update'].apply(
            lambda x: datetime.strptime(str(x)[:19], '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d %H:%M:%S"))
        values = [tuple(x) for x in df.to_numpy()]  # only raw data
        print(f"{len(values)} rows inserted successfully in MySQL {tgt_schema}.{table_name} table...")
        return len(values)

    except mysql.connector.Error as error:
        print(f"Error: {error}")
        src_conn.rollback()
        src_conn.close()


def insert_data_postgres(src_conn,
                         src_cursor,
                         tgt_conn=None,
                         tgt_cursor=None,
                         src_schema=None,
                         tgt_schema=None,
                         table_name=None):
    sql_code = f"""select * from {src_schema}.{table_name}"""  # src table
    try:
        src_cursor.execute(sql_code)  # execute query in src
        data = src_cursor.fetchall()  # get data from src
        df = pd.DataFrame(data)  # convert it to DataFrame
        values = [tuple(x) for x in df.to_numpy()]  # extract data
        print(f"{len(values)} rows are ready to insert in postgres {tgt_schema}.{table_name} table...")
        return len(values)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        src_conn.rollback()
        src_conn.close()
