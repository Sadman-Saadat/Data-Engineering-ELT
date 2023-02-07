import os
import json
import psycopg2  # postgres DB library to execute sql
import mysql
from mysql import connector

config_fle = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')  # config file read
with open(config_fle) as f:
    config_fle_data = json.load(f)


class Credential:
    def __init__(self):
        try:
            # Source DB1 credential
            # ====================
            self.postgres_conn1 = psycopg2.connect(database=config_fle_data["src_db"],
                                                   user=config_fle_data["src_username"],
                                                   password=config_fle_data["src_password"],
                                                   host=config_fle_data["src_host"],
                                                   port=config_fle_data["src_port"]
                                                   )
            self.postgres_conn1_cursor = self.postgres_conn1.cursor()
            self.postgres_conn1_schema = config_fle_data["src_schema"]
        except psycopg2.Error as e:
            print(e.pgerror)
            print(e.diag.message_detail)
            print("connection error. Please check Postgres source connection detail & credential.")

        try:
            # target DB1 credential
            # =====================
            self.postgres_conn2 = psycopg2.connect(database=config_fle_data["target_postgres_db"],
                                                   user=config_fle_data["target_postgres_db_username"],
                                                   password=config_fle_data["target_postgres_db_password"],
                                                   host=config_fle_data["target_postgres_db_host"],
                                                   port=config_fle_data["target_postgres_db_port"]
                                                   )
            self.postgres_conn2_cursor = self.postgres_conn2.cursor()
            self.postgres_conn2_schema = config_fle_data["target_postgres_schema"]
        except psycopg2.Error as e:
            print(e.pgerror)
            print(e.diag.message_detail)
            print("connection error. Please check Postgres target connection detail & credential.")

        try:
            # src/target DB2 credential
            # =========================
            self.mysql_conn1 = connector.connect(database=config_fle_data["mysql_db"],
                                                 host=config_fle_data["mysql_db_host"],
                                                 user=config_fle_data["mysql_db_username"],
                                                 password=config_fle_data["mysql_db_password"]
                                                 )
            self.mysql_conn1_cursor = self.mysql_conn1.cursor()
            self.mysql_conn1_schema = config_fle_data["mysql_schema"]
        except mysql.connector.Error as error:
            print(error)
            print("connection error. Please check MySQL source connection detail & credential.")

        try:
            # target DB2 credential
            # =========================
            self.mysql_conn2 = connector.connect(database=config_fle_data["target_mysql_db"],
                                                 host=config_fle_data["target_mysql_db_host"],
                                                 user=config_fle_data["target_mysql_db_username"],
                                                 password=config_fle_data["target_mysql_db_password"]
                                                 )
            self.mysql_conn2_cursor = self.mysql_conn2.cursor()
            self.mysql_conn2_schema = config_fle_data["target_mysql_schema"]
        except mysql.connector.Error as error:
            print(error)
            print("connection error. Please check MySQL target connection detail & credential.")
