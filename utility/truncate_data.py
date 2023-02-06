def truncate_data(insert_data):
    def wrapper(*args):
        print(args)
        insert_data()

    return wrapper


@truncate_data
def insert_data_fn():
    print('insert data functionality ...')


insert_data_fn(1, 2, 3, 4)
