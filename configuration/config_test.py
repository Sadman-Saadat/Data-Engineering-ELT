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

    """
    ============
    Test Purpose
    ============ 
    """

    def src_db_connection(self,
                          src_db,
                          src_username,
                          src_password,
                          src_host,
                          src_port):
        try:
            self.src_postgres = psycopg2.connect(database=src_db,
                                                 user=src_username,
                                                 password=src_password,
                                                 host=src_host,
                                                 port=src_port
                                                 )
            self.src_postgres_cursor = self.src_postgres.cursor()
            return True
        except psycopg2.Error as e:
            # print(e.diag.message_detail)
            # print("connection error. Please check DWH connection detail & credential.")
            return False

    def tgt_db1_connection(self,
                           tgt_db1,
                           tgt_db1_username,
                           tgt_db1_password,
                           tgt_db1_host,
                           tgt_db1_port):
        try:
            self.tgt_postgres = psycopg2.connect(database=tgt_db1,
                                                 user=tgt_db1_username,
                                                 password=tgt_db1_password,
                                                 host=tgt_db1_host,
                                                 port=tgt_db1_port
                                                 )
            self.tgt_postgres_cursor = self.tgt_postgres.cursor()
            return True
        except psycopg2.Error as e:
            # print(e.diag.message_detail)
            # print("connection error. Please check DWH connection detail & credential.")
            return False

    def tgt_db2_connection(self,
                           tgt_db2,
                           tgt_db2_username,
                           tgt_db2_password,
                           tgt_db2_host):
        try:
            self.tgt_mysql = connector.connect(database=tgt_db2,
                                          host=tgt_db2_host,
                                          user=tgt_db2_username,
                                          password=tgt_db2_password,
                                          )
            self.tgt_mysql_cursor = self.tgt_mysql.cursor()
            return True
        except mysql.connector.Error as e:
            # print(e.sqlstate)
            # print(e.diag.message_detail)
            # print("connection error. Please check DWH connection detail & credential.")
            return int(e.sqlstate)
