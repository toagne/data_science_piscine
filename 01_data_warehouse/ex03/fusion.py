import psycopg2

def clean_items_table(cursor):
    cursor.execute("""SELECT COUNT(DISTINCT product_id) FROM items""")
    n_of_single_items_in_original_list = cursor.fetchone()[0]
    cursor.execute("""
        CREATE TEMP TABLE new_items AS(
        SELECT
            product_id,
            MAX(NULLIF(category_id::text, ''))::bigint AS category_id,
            MAX(NULLIF(category_code, '')) AS category_code,
            MAX(NULLIF(brand, '')) AS brand
        FROM items
        GROUP BY product_id
        )""")
    cursor.execute("TRUNCATE TABLE items")
    cursor.execute("INSERT INTO items SELECT * from new_items")
    cursor.execute("""SELECT COUNT(*) FROM items""")
    n_of_items_in_new_list = cursor.fetchone()[0]
    cursor.execute("""
                    SELECT product_id
                    FROM items
                    GROUP BY product_id
                    HAVING COUNT(*) > 1""")
    duplicates = cursor.fetchall()
    count_of_duplicates_in_new_table = len(duplicates)
    if n_of_single_items_in_original_list != n_of_items_in_new_list or count_of_duplicates_in_new_table != 0:
        print('failed to clean item table')
    else:
        print('item table cleand succesfully')

def combine_tables(cursor):
    cursor.execute("""SELECT COUNT(*) FROM customers""")
    len_customers = cursor.fetchone()[0]
    cursor.execute("""
                    CREATE TABLE temp_customers AS (
                        SELECT * FROM customers
                        LEFT JOIN items USINg (product_id))""")
    cursor.execute("""ALTER TABLE customers RENAME TO customers_old""")
    cursor.execute("""ALTER TABLE temp_customers RENAME TO customers""")
    cursor.execute("""DROP TABLE customers_old""")
    cursor.execute("""SELECT COUNT(*) FROM customers""")
    new_len_customers = cursor.fetchone()[0]
    if len_customers != new_len_customers:
        print('failed to combine tables')
    else:
        print('tables combined succesfully')

def fusion():
    conn = psycopg2.connect(database="piscineds",
                            user='mpellegr', password='mysecretpassword', 
                            host='localhost', port='5432'
    )
    cursor = conn.cursor()
    clean_items_table(cursor)
    combine_tables(cursor)
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
    fusion()