import unittest
from configuration.config_test import Credential

credential = Credential()


class TestETL(unittest.TestCase):
    def test_postgres_connection(self):
        print(f"Postgres Connection testing started ...")
        self.assertEqual(credential.postgres_connection('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5432'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.postgres_connection('dvdrenta', 'postgres', 'postgres', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.postgres_connection('dvdrental', 'postgres', 'postgre', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.postgres_connection('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5431'), False)

        print(f"Postgres Connection testing done ...")

    def test_mysql_connection1(self):
        print(f"Source / Target MySQL Connection testing started ...")
        self.assertEqual(credential.mysql_connection('dvdrental', 'root', 'riju000346>R#', '127.0.0.1'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.mysql_connection('dvdrental', 'roo', 'riju000346>R#', '127.0.0.1'), 28000)
        self.assertEqual(credential.mysql_connection('dvdrental', 'root', 'riju000346>R', '127.0.0.1'), 28000)
        self.assertEqual(credential.mysql_connection('dvdrental', 'root', 'riju000346>R', '127.0.0.3'), 28000)

        print(f"Target MySQL Connection testing done ...")

    def test_mysql_connection2(self):
        print(f"Target MySQL Connection testing started ...")
        self.assertEqual(credential.mysql_connection('target_dvdrental', 'root', 'riju000346>R#', '127.0.0.1'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.mysql_connection('target_dvdrental', 'roo', 'riju000346>R#', '127.0.0.1'), 28000)
        self.assertEqual(credential.mysql_connection('target_dvdrental', 'root', 'riju000346>R', '127.0.0.1'), 28000)
        self.assertEqual(credential.mysql_connection('target_dvdrental', 'root', 'riju000346>R', '127.0.0.3'), 28000)

        print(f"Target MySQL Connection testing done ...")


if __name__ == '__main__':
    unittest.main()
