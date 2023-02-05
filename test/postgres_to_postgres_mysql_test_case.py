import unittest
from configuration.config_test import Credential
from utility import insert_data_test as idt

credential = Credential()


class TestETL(unittest.TestCase, Credential):

    def test_postgres_to_postgres_etl(self):
        print(f"Postgres ETL testing started ...")
        print("=================================")
        self.assertGreater(idt.insert_data_postgres(credential.postgres_conn1,
                                                    credential.postgres_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'actor')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.postgres_conn1,
                                                    credential.postgres_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'address')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.postgres_conn1,
                                                    credential.postgres_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'category')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.postgres_conn1,
                                                    credential.postgres_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'city')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.postgres_conn1,
                                                    credential.postgres_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'country')
                           , 0, 'new data')

        print("===============================")
        print(f"Postgres ETL testing done ...")

    def test_postgres_to_mysql_etl(self):
        print(f"MySQL ETL testing started ...")
        print("=================================")
        self.assertGreater(idt.insert_data_mysql(credential.postgres_conn1,
                                                 credential.postgres_conn1_cursor,
                                                 credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 'actor')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.postgres_conn1,
                                                 credential.postgres_conn1_cursor,
                                                 credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 'address')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.postgres_conn1,
                                                 credential.postgres_conn1_cursor,
                                                 credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 'category')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.postgres_conn1,
                                                 credential.postgres_conn1_cursor,
                                                 credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 'city')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.postgres_conn1,
                                                 credential.postgres_conn1_cursor,
                                                 credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 'country')
                           , 0, 'new data')

        print("=================================")
        print(f"MySQL ETL testing done ...")


if __name__ == '__main__':
    unittest.main()
