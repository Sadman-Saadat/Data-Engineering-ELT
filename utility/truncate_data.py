def truncate_data(insert_data):
    def wrapper(*args):
        _, _, tgt_conn, tgt_cur, _, tgt_schema, tgt_table = args  # src_conn, src_cur, tgt_conn, tgt_cur, tgt_table
        truncate_sql = f"""TRUNCATE TABLE {tgt_schema}.{tgt_table} RESTART IDENTITY CASCADE;"""
        tgt_cur.execute(truncate_sql)
        tgt_conn.commit()
        print(f"{tgt_schema}.{tgt_table} table data is truncated ...")
        insert_data(*args)

    return wrapper
