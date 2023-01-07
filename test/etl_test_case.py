import unittest
from configuration.config_test import Credential

credential = Credential()


class TestETL(unittest.TestCase):
    def test_postgres_connection1(self):
        print(f"Source Postgres Connection testing started ...")
        self.assertEqual(credential.postgres_connection1('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5432'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.postgres_connection1('dvdrenta', 'postgres', 'postgres', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.postgres_connection1('dvdrental', 'postgres', 'postgre', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.postgres_connection1('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5431'), False)

        print(f"Source Postgres Connection testing done ...")

    def test_postgres_connection2(self):
        print(f"Target Postgres DB Connection testing started ...")
        self.assertEqual(credential.postgres_connection2('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5432'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.postgres_connection2('dvdrenta', 'postgres', 'postgres', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.postgres_connection2('dvdrental', 'postgre', 'postgres', '127.0.0.1', '5432'), False)
        self.assertEqual(credential.postgres_connection2('dvdrental', 'postgres', 'postgres', '127.0.0.1', '5431'), False)

        print(f"Target Postgres Connection testing done ...")

    def test_mysql_connection1(self):
        print(f"Target MySQL Connection testing started ...")
        self.assertEqual(credential.mysql_connection1('dvdrental', 'root', 'riju000346>R#', '127.0.0.1'), True)
        """
        =================
        wrong credential 
        =================
        """
        self.assertEqual(credential.mysql_connection1('dvdrental', 'roo', 'riju000346>R#', '127.0.0.1'), 28000)
        self.assertEqual(credential.mysql_connection1('dvdrental', 'root', 'riju000346>R', '127.0.0.1'), 28000)
        self.assertEqual(credential.mysql_connection1('dvdrental', 'root', 'riju000346>R', '127.0.0.3'), 28000)

        print(f"Target MySQL Connection testing done ...")


if __name__ == '__main__':
    unittest.main()
