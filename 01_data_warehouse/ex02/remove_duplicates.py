import psycopg2

def calculate_n_of_duplicates(cursor):
    cursor.execute("""WITH grp AS (
                        SELECT COUNT(*) as cnt
                            FROM customers
                        GROUP BY event_time, event_type, product_id, price, user_id, user_session
                        HAVING COUNT(*)>1
                    )
                    SELECT SUM(cnt) - COUNT(*) AS extra_rows
                    FROM grp""")
    if type(cursor.fetchone()[0]).__name__ == 'NoneType':
        return 0
    return cursor.fetchone()[0]

def create_new_table_without_duplicates(cursor):
    cursor.execute("""CREATE TABLE temp AS SELECT DISTINCT * from customers""")
    cursor.execute("""SELECT COUNT(*) FROM customers""")
    tot_len = cursor.fetchone()[0]
    cursor.execute("""SELECT COUNT(*) FROM temp""")
    temp_len = cursor.fetchone()[0]
    dups_len = calculate_n_of_duplicates(cursor)
    if tot_len - dups_len != temp_len:
        print('duplicates not removed correctly')
        exit()
    cursor.execute("""ALTER TABLE customers RENAME TO customers_old""")
    cursor.execute("""ALTER TABLE temp RENAME TO customers""")
    cursor.execute("""DROP TABLE customers_old""")

def remove_bug_values(cursor):
    cursor.execute("""WITH duplicates AS (
                        SELECT b.ctid as ctid_to_delete
                        FROM customers a
                        JOIN customers b
                        ON a.event_type = b.event_type
                            AND a.product_id = b.product_id
                            AND a.price = b.price
                            AND a.user_id = b.user_id
                            AND a.user_session = b.user_session
                            AND a.event_time < b.event_time
                            AND EXTRACT(EPOCH FROM b.event_time - a.event_time) <= 1
                    )
                    DELETE FROM customers WHERE ctid IN (SELECT ctid_to_delete FROM duplicates)
                    """)

def check_for_unexpected_values(cursor):
    cursor.execute("""SELECT COUNT(*) FROM CUSTOMERS
                    GROUP BY event_time, event_type, product_id, price, user_id, user_session
                    HAVING COUNT(*) > 1""")
    if len(cursor.fetchall()) != 0:
        print('Error: there are still duplicate lines in the database')
    cursor.execute("""SELECT b.ctid
                        FROM customers a
                        JOIN customers b
                        ON a.event_type = b.event_type
                            AND a.product_id = b.product_id
                            AND a.price = b.price
                            AND a.user_id = b.user_id
                            AND a.user_session = b.user_session
                            AND a.event_time < b.event_time
                            AND EXTRACT(EPOCH FROM b.event_time - a.event_time) <= 1""")
    if len(cursor.fetchall()) != 0:
        print('Error: there are still duplicate lines in the database')

def remove_duplicates():
    conn = psycopg2.connect(database="piscineds",
                            user='mpellegr', password='mysecretpassword', 
                            host='localhost', port='5432'
    )
    cursor = conn.cursor()
    try:
        create_new_table_without_duplicates(cursor)
        remove_bug_values(cursor)
        check_for_unexpected_values(cursor)
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    remove_duplicates()