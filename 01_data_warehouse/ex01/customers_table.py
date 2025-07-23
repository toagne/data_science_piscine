import psycopg2

def create_table(cursor):
	sql = '''CREATE TABLE IF NOT EXISTS customers(
		event_time TIMESTAMP WITH TIME ZONE,
		event_type TEXT, 
		product_id INTEGER,
		price REAL,
		user_id BIGINT,
		user_session UUID
	);'''
	cursor.execute(sql)

def merge_tables(cursor):
    cursor.execute("""TRUNCATE TABLE customers""")
    cursor.execute("""SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'data_202%'""")
    sum_of_tables = 0
    for name in cursor.fetchall():
        cursor.execute(f'SELECT COUNT(*) FROM "{name[0]}"')
        temp_len = cursor.fetchone()[0]
        sum_of_tables += temp_len
        cursor.execute(f'INSERT INTO customers SELECT * from "{name[0]}"')
    cursor.execute("""SELECT COUNT(*) FROM customers""")
    len_customers = cursor.fetchone()[0]
    if len_customers != sum_of_tables:
        print('something went wrong in the tables merging')
        exit()

def customers_table():
    conn = psycopg2.connect(database="piscineds",
                            user='mpellegr', password='mysecretpassword', 
                            host='localhost', port='5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    create_table(cursor)
    merge_tables(cursor)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    customers_table()