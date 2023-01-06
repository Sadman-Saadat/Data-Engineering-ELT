import unittest
from configuration.config_test import Credential

credential = Credential()


class TestETL(unittest.TestCase):
    def test_src_postgres_connection(self):
        print(f"Source Postgres Connection testing started ...")
        self.assertEqual(credential.src_db_connection('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5432'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.src_db_connection('dvdrenta', 'postgres', 'postgres', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.src_db_connection('dvdrental', 'postgres', 'postgre', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.src_db_connection('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5431'), False)

        print(f"Source Postgres Connection testing done ...")

    def test_tgt_db1_postgres_connection(self):
        print(f"Target Postgres DB Connection testing started ...")
        self.assertEqual(credential.tgt_db1_connection('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5432'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.tgt_db1_connection('dvdrenta', 'postgres', 'postgres', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.tgt_db1_connection('dvdrental', 'postgre', 'postgres', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.tgt_db1_connection('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5431'), False)

        print(f"Target Postgres Connection testing done ...")

    def test_tgt_db2_mysql_connection(self):
        print(f"Target MySQL Connection testing started ...")
        self.assertEqual(credential.tgt_db2_connection('dvdrental', 'root', 'riju000346>R#', '127.0.0.1'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.tgt_db2_connection('dvdrental', 'roo', 'riju000346>R#', '127.0.0.1'), 28000)
        self.assertEqual(credential.tgt_db2_connection('dvdrental', 'root', 'riju000346>R', '127.0.0.1'), 28000)
        self.assertEqual(credential.tgt_db2_connection('dvdrental', 'root', 'riju000346>R', '127.0.0.3'), 28000)

        print(f"Target MySQL Connection testing done ...")


if __name__ == '__main__':
    unittest.main()
