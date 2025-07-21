import psycopg2

def remove_duplicates():
    conn = psycopg2.connect(database="piscineds",
                            user='mpellegr', password='mysecretpassword', 
                            host='localhost', port='5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    # cursor.execute("""SELECT event_time, event_type, product_id, price, user_id, user_session,
    #                 COUNT(*) FROM customers
    #                 GROUP BY event_time, event_type, product_id, price, user_id, user_session
    #                 HAVING COUNT(*)>1""")
    cursor.execute("""SELECT a.ctid
                    FROM data_2022_oct a
                    JOIN data_2022_oct b
                    ON a.event_type = b.event_type
                    AND a.product_id = b.product_id
                    AND a.price = b.price
                    AND a.user_id = b.user_id
                    AND a.user_session = b.user_session
                    AND a.event_time <> b.event_time
                    AND ABS(EXTRACT(EPOCH FROM a.event_time - b.event_time)) <= 1
                    ORDER BY a.event_time;
                    """)
    dups = cursor.fetchall()
    i = 0
    for line in dups:
        print(line)
        i += 1
        if i == 10:
            break
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    remove_duplicates()