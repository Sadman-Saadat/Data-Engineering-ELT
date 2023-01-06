# ELT process: Extract data from postgres then load the data into Postgres & MySQL DB.

# Dir structure

+ **configuration**: all kinds of DB credential
+ **utility**      : all helper function
+ **test**         : test case for unit testing 

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
    pip freeze -r requirements.txt
    ```