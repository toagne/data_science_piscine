import psycopg2

conn = psycopg2.connect(database="piscineds",
                        user='mpellegr', password='mysecretpassword', 
                        host='localhost', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()

sql = '''CREATE TABLE IF NOT EXISTS data_2022_oct(
	event_time TIMESTAMP WITH TIME ZONE,
	event_type TEXT, 
	product_id INTEGER,
	price REAL,
	user_id BIGINT,
	user_session UUID
);'''

cursor.execute(sql)

csv_path = '/Users/mpellegr/hive/data_science_piscine/00_creation_of_a_db/subject/customer/data_2022_oct.csv'
with open(csv_path) as file:
    cursor.copy_expert("""COPY data_2022_oct FROM STDIN WITH CSV HEADER DELIMITER ','""", file)
    # data = file.read()

sql3 = '''select * from data_2022_oct LIMIT 5;'''
cursor.execute(sql3)
for i in cursor.fetchall():
    print(i)

conn.commit()
conn.close()