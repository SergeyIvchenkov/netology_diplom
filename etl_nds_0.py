import random
import psycopg2
import pandas as pd

 #Скрипт первоначально заполняет таблицы nds-слоя данными (без nds.invoice)

#Чтение csv файла
df = pd.read_csv('supermarket_sales - Sheet1.csv', delimiter = ',')
# Преобразование названий столбцов к нижнему регистру
df.columns = [col.lower() for col in df.columns]

# Подключение к базе данных Postgres
conn = psycopg2.connect(dbname='postgres', user='postgres', password='sergio328', host='localhost')

try:
    # Создание курсора
    with conn.cursor() as curs:
        # Получение уникальных значений из csv для nds.branch
        branch_unique_pairs = df[['branch', 'city']].drop_duplicates().values.tolist()
        for pair in branch_unique_pairs:
            branch, city = pair
            curs.execute("INSERT INTO nds.branch VALUES (default, %s, %s)", (branch, city))


        #вставим 50 клиентов и случайно распределим
        customer_unique_pairs = df[['gender', 'customer type']].drop_duplicates().values.tolist()
        for i in range(50):
            c_gender, c_customer_type = random.choice(customer_unique_pairs)
            curs.execute("insert into nds.customer values(default, %s, %s, %s, %s)", (c_customer_type, c_gender, '2018-01-01', '5999-12-31'))

        #Заполним nds.product_line
        pr_line_names = df['product line'].unique().tolist()
        for pr_line_name in pr_line_names:
            curs.execute("insert into nds.product_line(product_line_name) values (%s)", (pr_line_name, ))

        #заполняем nds.product
        product_list = df[['product line', 'unit price', 'gross margin percentage']].drop_duplicates().values.tolist()
        for pr in product_list:
            curs.execute("select product_line_id from nds.product_line t where t.product_line_name = %s", (pr[0],))
            pr_line_id = curs.fetchone()[0]
            pr_unit_price = pr[1]
            pr_gross_percent = pr[2]
            curs.execute("insert into nds.product values(default, %s, %s, %s, %s, %s)", (pr_line_id, pr_unit_price, pr_gross_percent, '2019-01-01', '5999-12-31'))

        conn.commit()

finally:
    # Закрытие соединения с БД
    conn.close()