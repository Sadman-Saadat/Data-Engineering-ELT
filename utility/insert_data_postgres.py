import psycopg2
from psycopg2 import extras
import pandas as pd


def insert_data_postgres(src_conn, src_cursor, tgt_conn=None, tgt_cursor=None, table_name=None):
    sql_code = f"""select * from public.{table_name}"""  # src table
    try:
        src_cursor.execute(sql_code)  # execute query in src
        data = src_cursor.fetchall()  # get data from src
        df = pd.DataFrame(data)  # convert it to DataFrame
        values = [tuple(x) for x in df.to_numpy()]  # extract data
        cols = ",".join([col[0] for col in src_cursor.description])  # columns
        insert_query = "insert into etl.%s(%s) values %%s" % (table_name, cols)  # target table
        extras.execute_values(tgt_cursor, insert_query, values)  # execute query in target
        tgt_conn.commit()  # commit
        # tgt_conn.close()
        print(f"{len(values)} rows inserted successfully in etl.{table_name} table...")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        src_conn.rollback()
        # self.src.close()
