import psycopg2
from psycopg2 import extras
import pandas as pd
import numpy as np
from datetime import datetime
import mysql
from utility.truncate_data import truncate_data


@truncate_data
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
        df = df.convert_dtypes() # acctual data types 
        df.replace({np.nan: None}, inplace = True)  # replace null value
        values = [tuple(x) for x in df.to_numpy()]  # extract data
        cols = ",".join([col[0] for col in src_cursor.description])  # columns
        insert_query = f"insert into {tgt_schema}.%s(%s) values %%s" % (table_name, cols)  # target table
        extras.execute_values(tgt_cursor, insert_query, values)  # execute query in target
        tgt_conn.commit()  # commit
        print(f"{len(values)} rows inserted successfully in {tgt_schema}.{table_name} table...")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        src_conn.rollback()
        src_conn.close()


@truncate_data
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
        values = [tuple(x) for x in df.to_numpy()]
        insert_query = f"""insert into {tgt_schema}.{table_name} values ({','.join(['%s' for _ in range(len(cols.split(',')))])})"""  # cols.split(): the more feature the more value for each row
        tgt_cursor.executemany(insert_query, values)  # execute query in target
        tgt_conn.commit()  # commit
        print(f"{len(values)} rows inserted successfully in {tgt_schema}.{table_name} table...")

    except mysql.connector.Error as error:
        print(f"Error: {error}")
        src_conn.rollback()
        src_conn.close()


def insert_data_mongo(src_conn, src_cursor, tgt_conn=None, table_name=None):
    sql_code = f"""select * from public.{table_name}"""  # src table
    try:
        src_cursor.execute(sql_code)  # execute query in src
        data = src_cursor.fetchall()  # get data from src
        cols = ",".join([col[0] for col in src_cursor.description])  # columns
        cols = tuple(cols.split(','))
        collection = tgt_conn[table_name]
        deleted_doc = collection.delete_many({})  # delete all doc before inserted
        print(f"Total deleted doc: {deleted_doc.deleted_count}")
        for i in data:
            row = i
            col = cols
            doc = {k: v for k, v in zip(col, row)}
            id_key = table_name + '_id'  # table primary key
            doc_id = {'_id': doc[id_key]}
            doc = {**doc, **doc_id}
            insert_doc = collection.insert_one(doc)
        print(f"Total inserted doc: {len(data)}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        src_conn.rollback()
        src_conn.close()
