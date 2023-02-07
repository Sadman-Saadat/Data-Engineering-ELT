import unittest
from configuration.config_test import Credential
from utility import insert_data_mysql_to_mysql_postgres_test as idt

credential = Credential()


class TestETL(unittest.TestCase):

    def test_mysql_to_mysql_etl(self):
        print(f"MySQL ETL testing started ...")
        print("=================================")
        self.assertGreater(idt.insert_data_mysql(credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 credential.mysql_conn2,
                                                 credential.mysql_conn2_cursor,
                                                 'dvdrental',
                                                 'target_dvdrental',
                                                 'actor')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 credential.mysql_conn2,
                                                 credential.mysql_conn2_cursor,
                                                 'dvdrental',
                                                 'target_dvdrental',
                                                 'address')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 credential.mysql_conn2,
                                                 credential.mysql_conn2_cursor,
                                                 'dvdrental',
                                                 'target_dvdrental',
                                                 'category')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 credential.mysql_conn2,
                                                 credential.mysql_conn2_cursor,
                                                 'dvdrental',
                                                 'target_dvdrental',
                                                 'city')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_mysql(credential.mysql_conn1,
                                                 credential.mysql_conn1_cursor,
                                                 credential.mysql_conn2,
                                                 credential.mysql_conn2_cursor,
                                                 'dvdrental',
                                                 'target_dvdrental',
                                                 'country')
                           , 0, 'new data')

        print("===============================")
        print(f"MySQL ETL testing done ...")

    def test_mysql_to_postgres_etl(self):
        print(f"Postgres ETL testing started ...")
        print("=================================")
        self.assertGreater(idt.insert_data_postgres(credential.mysql_conn1,
                                                    credential.mysql_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'dvdrental',
                                                    'etl',
                                                    'actor')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.mysql_conn1,
                                                    credential.mysql_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'dvdrental',
                                                    'etl',
                                                    'address')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.mysql_conn1,
                                                    credential.mysql_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'dvdrental',
                                                    'etl',
                                                    'category')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.mysql_conn1,
                                                    credential.mysql_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'dvdrental',
                                                    'etl',
                                                    'city')
                           , 0, 'new data')
        self.assertGreater(idt.insert_data_postgres(credential.mysql_conn1,
                                                    credential.mysql_conn1_cursor,
                                                    credential.postgres_conn2,
                                                    credential.postgres_conn2_cursor,
                                                    'dvdrental',
                                                    'etl',
                                                    'country')
                           , 0, 'new data')

        print("=================================")
        print(f"Postgres ETL testing done ...")


if __name__ == '__main__':
    unittest.main()
