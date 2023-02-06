from configuration.config import Credential
from utility.insert_data_mysql_to_mysql_postgres import insert_data_mysql, insert_data_postgres


class ETL(Credential):
    def __init__(self):
        super(ETL, self).__init__()

    # table 1
    def actor_table_insert_data(self):
        table_name = "actor"
        insert_data_postgres(self.mysql_conn1, self.mysql_conn1_cursor, self.postgres_conn2,
                             self.postgres_conn2_cursor, table_name)
        insert_data_mysql(self.mysql_conn1, self.mysql_conn1_cursor, self.mysql_conn2,
                          self.mysql_conn2_cursor, table_name)

    # table 2
    def address_table_insert_data(self):
        table_name = "address"
        insert_data_postgres(self.mysql_conn1, self.mysql_conn1_cursor, self.postgres_conn2,
                             self.postgres_conn2_cursor, table_name)
        insert_data_mysql(self.mysql_conn1, self.mysql_conn1_cursor, self.mysql_conn2,
                          self.mysql_conn2_cursor, table_name)

    # table 3
    def category_table_insert_data(self):
        table_name = "category"
        insert_data_postgres(self.mysql_conn1, self.mysql_conn1_cursor, self.postgres_conn2,
                             self.postgres_conn2_cursor, table_name)
        insert_data_mysql(self.mysql_conn1, self.mysql_conn1_cursor, self.mysql_conn2,
                          self.mysql_conn2_cursor, table_name)

    # table 4
    def city_table_insert_data(self):
        table_name = "city"
        insert_data_postgres(self.mysql_conn1, self.mysql_conn1_cursor, self.postgres_conn2,
                             self.postgres_conn2_cursor, table_name)
        insert_data_mysql(self.mysql_conn1, self.mysql_conn1_cursor, self.mysql_conn2,
                          self.mysql_conn2_cursor, table_name)

    # table 5
    def country_table_insert_data(self):
        table_name = "country"
        insert_data_postgres(self.mysql_conn1, self.mysql_conn1_cursor, self.postgres_conn2,
                             self.postgres_conn2_cursor, table_name)
        insert_data_mysql(self.mysql_conn1, self.mysql_conn1_cursor, self.mysql_conn2,
                          self.mysql_conn2_cursor, table_name)

    def main(self):
        self.actor_table_insert_data()
        self.address_table_insert_data()
        self.category_table_insert_data()
        self.city_table_insert_data()
        self.country_table_insert_data()

        self.mysql_conn1.close()
        self.mysql_conn2.close()
        self.postgres_conn2.close()


if __name__ == '__main__':
    etl = ETL()
    etl.main()
