import psycopg2
import os
import csv


CSV_DATA_PATH = os.path.join('north_data')


def file_to_db(path, file, table):

    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='12345')
    try:
        with conn:
            with conn.cursor() as cur:

                with open(f"{path}/{file}", 'r') as file:
                    reader = csv.reader(file)
                    next(reader)

                    for row in reader:
                        # Вставляем данные в таблицу
                        insert_query = f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(row))})"
                        cur.execute(insert_query, row)
                        conn.commit()

                cur.execute(f"SELECT * FROM {table}")
                rows = cur.fetchall()

                for row in rows:
                    print(row)

    finally:
        conn.close()


if __name__ == '__main__':
    file_to_db(CSV_DATA_PATH, 'customers_data.csv', 'customers')
    file_to_db(CSV_DATA_PATH, 'employees_data.csv', 'employees')
    file_to_db(CSV_DATA_PATH, 'orders_data.csv', 'orders')
