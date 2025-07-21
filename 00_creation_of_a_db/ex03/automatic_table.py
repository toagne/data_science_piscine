import os
import psycopg2

def create_table(cursor, table_name: str):
	print(f'Creating table {table_name[1:-1]}...')
	sql = f'''CREATE TABLE IF NOT EXISTS {table_name} (
		event_time TIMESTAMP WITH TIME ZONE,
		event_type TEXT, 
		product_id INTEGER,
		price REAL,
		user_id BIGINT,
		user_session UUID
	);'''
	cursor.execute(sql)
	print(f'Table {table_name[1:-1]} has been created')

def copy_csv(cursor, table_name):
	print(f'Deleting previous content of the table {table_name[1:-1]} if existing...')
	cursor.execute(f'TRUNCATE TABLE {table_name[1:-1]}')
	print(f'Copying {table_name[1:-1]}.csv into table {table_name[1:-1]}')
	csv_path = f'/Users/mpellegr/hive/data_science_piscine/00_creation_of_a_db/subject/customer/{table_name[1:-1]}.csv'
	with open(csv_path) as file:
		cursor.copy_expert(f"""COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','""", file)
	print('Copy completed')

def automatic_table():
	conn = psycopg2.connect(database="piscineds",
							user='mpellegr', password='mysecretpassword', 
							host='localhost', port='5432'
	)
	conn.autocommit = True
	cursor = conn.cursor()
	for file in os.listdir('../subject/customer'):
		filename = file[:-4]
		table_name = f'"{filename}"'
		create_table(cursor, table_name)
		copy_csv(cursor, table_name)
		print('********************')
	conn.commit()
	conn.close()

if __name__ == '__main__':
	automatic_table()