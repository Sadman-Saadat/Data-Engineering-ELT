def truncate_data(insert_data):
    def wrapper(*args):
        _, _, tgt_conn, tgt_cur, tgt_table = args  # src_conn, src_cur, tgt_conn, tgt_cur, tgt_table
        truncate_sql = f"""truncate table target_dvdrental.{tgt_table} ;"""
        tgt_cur.execute(truncate_sql)
        tgt_conn.commit()
        print(f"{tgt_table} table data is truncated ...")
        insert_data(*args)

    return wrapper
