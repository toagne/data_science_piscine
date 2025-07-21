import psycopg2

def create_table(cursor):
    sql = '''CREATE TABLE IF NOT EXISTS items(
        product_id INTEGER,
        category_id BIGINT,
        category_code TEXT,
        brand TEXT
        )'''
    cursor.execute(sql)

def copy_csv(cursor):
    csv_path = '../subject/item/item.csv'
    with open(csv_path) as file:
        cursor.copy_expert("""COPY items FROM STDIN WITH CSV HEADER DELIMITER ','""", file)

def items_table():
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
    items_table()