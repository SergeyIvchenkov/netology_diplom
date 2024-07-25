import datetime
import random
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

#скрипт заполняет nds.invoice

#Чтение из файла
df = pd.read_csv('supermarket_sales - Sheet1.csv', delimiter = ',')
# Преобразование названий столбцов к нижнему регистру
df.columns = [col.lower().strip() for col in df.columns]

# Подключение к базе данных Postgres
conn = psycopg2.connect(dbname='postgres', user='postgres', password='sergio328', host='localhost')
conn_string = 'postgresql://postgres:sergio328@localhost/postgres'
db = create_engine(conn_string)
conn_1 = db.connect()

try:
    # Создание курсора
    with conn.cursor() as curs:
        # Получение данных из таблицы nds.customer
        curs.execute("SELECT * FROM nds.customer")
        table_customer = curs.fetchall()
        df_customer = pd.DataFrame(table_customer, columns=[col[0] for col in curs.description])

        # Получение данных из таблицы nds.branch
        curs.execute("SELECT * FROM nds.branch")
        table_branch = curs.fetchall()
        df_branch = pd.DataFrame(table_branch, columns=[col[0] for col in curs.description])

        # Получение данных из таблицы nds.product
        curs.execute("""
                     WITH cte AS (
                       SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY unit_price ORDER BY random()) AS rn
                       FROM nds.product p 
                       join nds.product_line pl on pl.product_line_id = p.product_line_id)
                     SELECT * 
                     FROM cte 
                     where rn = 1
                     """)
        table_product = curs.fetchall()
        df_product = pd.DataFrame(table_product, columns=[col[0] for col in curs.description])

        # ---------------------------------
        # Добавление нового столбца в датафрейм, распределяем клиентов
        df['customer_id'] = df.apply(lambda row: df_customer.loc[(df_customer['customer_type'] == row['customer type']) & (
                    df_customer['gender'] == row['gender']), 'customer_id'].sample(1).item(), axis=1)

        # Добавление нового столбца в датафрейм, распределяем филиалы
        df['branch_id'] = df.apply(lambda row: df_branch.loc[(df_branch['branch_name'] == row['branch']), 'branch_id'].sample(1).item(),axis=1)

        # Добавление нового столбца в DataFrame, распределяем продукты
        df['unit price'] = df['unit price'].astype(float)
        df_product['unit_price'] = df_product['unit_price'].astype(float)
        df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
        df = df.merge(df_product, left_on='unit price', right_on='unit_price', how = 'left')

        #формируем конечную выборку перед вставкой в nds.invoice
        df = df[['invoice id', 'branch_id', 'customer_id', 'product_id', 'quantity', 'date', 'time', 'payment', 'rating']]
        df.to_csv('new.csv', header=False, index=False, mode='w')
        with open('new.csv', 'r') as f:
            curs.copy_expert("COPY nds.invoice (invoice_id, branch_id, customer_id, product_id, quantity, date, time, payment, rating) FROM STDIN WITH CSV HEADER", f)

        conn.commit()

finally:
    # Закрытие соединения с БД
    conn.close()
    print('end')