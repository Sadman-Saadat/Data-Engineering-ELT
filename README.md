# ELT (```Ongoing```)

# Dir structure
```
project
│   README.md
│   requirements.txt  
|   .gitignore  
|   postgres_to_postgres_mysql.py
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
│   │   etl_test_case.py : testing the elt project
│   
└───utility
    │   insert_data.py  : helper fn
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