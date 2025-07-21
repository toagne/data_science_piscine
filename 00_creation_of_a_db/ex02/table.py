import psycopg2

def create_table(cursor):
	sql = '''CREATE TABLE IF NOT EXISTS data_2022_oct(
		event_time TIMESTAMP WITH TIME ZONE,
		event_type TEXT, 
		product_id INTEGER,
		price REAL,
		user_id BIGINT,
		user_session UUID
	);'''
	cursor.execute(sql)

def copy_csv(cursor):
	csv_path = '/Users/mpellegr/hive/data_science_piscine/00_creation_of_a_db/subject/customer/data_2022_oct.csv'
	with open(csv_path) as file:
		cursor.copy_expert("""COPY data_2022_oct FROM STDIN WITH CSV HEADER DELIMITER ','""", file)


def table():
	conn = psycopg2.connect(database="piscineds",
							user='mpellegr', password='mysecretpassword', 
							host='localhost', port='5432'
	)
	conn.autocommit = True
	cursor = conn.cursor()
	create_table(cursor)
	copy_csv(cursor)
	conn.commit()
	conn.close()

if __name__ == '__main__':
    table()