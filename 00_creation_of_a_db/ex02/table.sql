CREATE TABLE IF NOT EXISTS data_2022_oct(
	event_time TIMESTAMP WITH TIME ZONE,
	event_type TEXT,
	product_id INTEGER,
	price REAL,
	user_id BIGINT,
	user_session UUID
)

\COPY data_2022_oct FROM '/Users/mpellegr/hive/data_science_piscine/00_creation_of_a_db/subject/customer/data_2022_oct.csv' HEADER DELIMITER ',' CSV