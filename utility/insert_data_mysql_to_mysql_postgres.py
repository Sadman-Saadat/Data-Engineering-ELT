import psycopg2
from psycopg2 import extras
import pandas as pd
from datetime import datetime
import mysql
from configuration.config import Credential

credential = Credential()


def insert_data_mysql(src_conn, src_cursor, tgt_conn=None, tgt_cursor=None, table_name=None):
    sql_code = f"""select * from {table_name}"""  # src table
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
        insert_query = f"""insert into {table_name} values ({','.join(['%s' for _ in range(len(cols.split(',')))])})"""  # cols.split(): more feature more value for row
        tgt_cursor.executemany(insert_query, values)  # execute query in target
        tgt_conn.commit()  # commit
        print(f"{len(values)} rows inserted successfully in MySQL target_dvdrental.{table_name} table...")

    except mysql.connector.Error as error:
        print(f"Error: {error}")
        src_conn.rollback()
        src_conn.close()


def insert_data_postgres(src_conn, src_cursor, tgt_conn=None, tgt_cursor=None, table_name=None):
    sql_code = f"""select * from {table_name}"""  # src table
    try:
        src_cursor.execute(sql_code)  # execute query in src
        data = src_cursor.fetchall()  # get data from src
        df = pd.DataFrame(data)  # convert it to DataFrame
        values = [tuple(x) for x in df.to_numpy()]  # extract data
        cols = ",".join([col[0] for col in src_cursor.description])  # columns
        insert_query = "insert into etl.%s(%s) values %%s" % (table_name, cols)  # target table
        extras.execute_values(tgt_cursor, insert_query, values)  # execute query in target
        tgt_conn.commit()  # commit
        print(f"{len(values)} rows are ready to insert in postgres etl.{table_name} table...")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        src_conn.rollback()
        src_conn.close()
