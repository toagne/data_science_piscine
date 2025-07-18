#! /bin/bash

TABLE_TEMPLATE="CREATE TABLE IF NOT EXISTS %s (
    event_time TIMESTAMPTZ,
    event_type TEXT,
    product_id INTEGER,
    price REAL,
    user_id BIGINT,
    user_session UUID
);"
DB_NAME="piscineds"
DB_USER="mpellegr"
DB_HOST="localhost"

export PGPASSWORD='mysecretpassword'

for file in ../subject/customer/*; do
	base=$(basename "$file" .csv)
	create_table_sql=$(printf "$TABLE_TEMPLATE" "$base")
	psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -c "$create_table_sql"
	psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -c "\COPY \"$base\" FROM '$file' WITH CSV HEADER"
done