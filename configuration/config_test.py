import os
import json
import psycopg2  # postgres DB library to execute sql

config_fle = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config_test.json')  # config file read
with open(config_fle) as f:
    config_fle_data = json.load(f)


class Credential:
    def __init__(self):
        try:
            # Source DB credential
            # ====================
            self.src = psycopg2.connect(database=config_fle_data["src_db"],
                                        user=config_fle_data["src_username"],
                                        password=config_fle_data["src_password"],
                                        host=config_fle_data["src_host"],
                                        port=config_fle_data["src_port"]
                                        )
            self.src_cursor = self.src.cursor()
        except psycopg2.Error as e:
            print(e.pgerror)
            print(e.diag.message_detail)
            print("connection error. Please check DWH connection detail & credential.")

        try:
            # DWH DB credential
            # =================
            self.dwh = psycopg2.connect(database=config_fle_data["target_db"],
                                        user=config_fle_data["target_db_username"],
                                        password=config_fle_data["target_db_password"],
                                        host=config_fle_data["target_db_host"],
                                        port=config_fle_data["target_db_port"]
                                        )
            self.dwh_cursor = self.dwh.cursor()
        except psycopg2.Error as e:
            print(e.pgerror)
            print(e.diag.message_detail)
            print("connection error. Please check Replica DB connection detail & credential.")

