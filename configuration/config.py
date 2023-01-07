import os
import json
import psycopg2  # postgres DB library to execute sql
import mysql    # mysql DB library to execute sql
from mysql import connector

config_fle = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')  # config file read
with open(config_fle) as f:
    config_fle_data = json.load(f)


class Credential:
    def __init__(self):
        try:
            # Source DB credential
            # ====================
            self.src_postgres = psycopg2.connect(database=config_fle_data["src_db"],
                                                 user=config_fle_data["src_username"],
                                                 password=config_fle_data["src_password"],
                                                 host=config_fle_data["src_host"],
                                                 port=config_fle_data["src_port"]
                                                 )
            self.src_postgres_cursor = self.src_postgres.cursor()
        except psycopg2.Error as e:
            print(e.pgerror)
            print(e.diag.message_detail)
            print("connection error. Please check DWH connection detail & credential.")

        try:
            # target DB1 credential
            # =====================
            self.tgt_postgres = psycopg2.connect(database=config_fle_data["target_postgres_db"],
                                                 user=config_fle_data["target_postgres_db_username"],
                                                 password=config_fle_data["target_postgres_db_password"],
                                                 host=config_fle_data["target_postgres_db_host"],
                                                 port=config_fle_data["target_postgres_db_port"]
                                                 )
            self.tgt_postgres_cursor = self.tgt_postgres.cursor()
        except psycopg2.Error as e:
            print(e.pgerror)
            print(e.diag.message_detail)
            print("connection error. Please check Replica DB connection detail & credential.")

        try:
            # target DB2 credential
            # =========================
            self.tgt_mysql = connector.connect(database=config_fle_data["target_mysql_db"],
                                               host=config_fle_data["target_mysql_db_host"],
                                               user=config_fle_data["target_mysql_db_username"],
                                               password=config_fle_data["target_mysql_db_password"]
                                               )
            self.tgt_mysql_cursor = self.tgt_mysql.cursor()
        except mysql.connector.Error as error:
            print(error)
            print("connection error. Please check Replica DB connection detail & credential.")