# ELT (```Ongoing```)

# Dir structure
```
project
│   README.md
│   requirements.txt  
|   .gitignore  
|   postgres_to_postgres_mysql_mongo.py
|   mysql_to_mysql_postgres.py
│
└───miscellaneous
│   │   dvdrental.tar : sample db
│
└───configuration
│   │   config.json     : all kinds of DB credential
│   │   config.py       : mapping the credential
│   │   config_test.py  : for testing
|   |
│   
└───test
│   │   configuration_test_case.py
│   │   mysql_to_mysql_postgres_test_case.py
│   │   postgres_to_postgres_mysql_test_case.py
│   
└───utility
    │   insert_data_mysql_to_mysql_postgres.py
    │   insert_data_mysql_to_mysql_postgres_test.py
    │   insert_data_postgres_to_postgres_mysql_test.py 
    │   insert_data_postgres_to_postgres_mysql_mongo.py
    │   insert_data_postgres_to_postgres_mysql_mongo.py
    │   truncate_data.py
```

# How to run the project

+ create a python virtual env

    ```text
    python -m venv envName
    ```
+ clone the repo into the venv
    ```text
    git clone https://github.com/riju18/Data-Engineering-ELT.git
    ```
+ install packages from ```requirements.txt```
    ```text
    pip install -r requirements.txt
    ```
+ ```configuration --> config.json```
    ```text
    Add/update source & target DB credentials
    ```