"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


def fill_postgres_from_csv(pass_word, table_name, file_name):
    """
    Функция заполняет таблицы БД Postgres из файла csv
    param: pass_word Пароль входа в БД
    param: table_name Название таблицы в БД
    param: file_name Название файла csv
    """
    # Подключение к базе данных
    conn = psycopg2.connect(
        host="localhost",
        database="north",
        user="postgres",
        password=pass_word
    )
    cur = conn.cursor()

    # Чтение данных из CSV файла и добавление их в базу данных
    with open(file_name, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Получаем заголовок
        columns = ', '.join(header)  # Преобразуем заголовок в строку столбцов
        placeholders = ', '.join(['%s'] * len(header))  # Получаем количество плейсхолдеров
        for row in reader:
            cur.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", row)

    conn.commit()
    print(f'Данные успешно внесены в таблицу {table_name}.')
    cur.close()
    conn.close()


password = input('Введите пароль для входа в БД postgreSQL:\n')

name_table = 'employees'
name_file = 'north_data/employees_data.csv'

fill_postgres_from_csv(password, name_table, name_file)

name_table = 'customers'
name_file = 'north_data/customers_data.csv'

fill_postgres_from_csv(password, name_table, name_file)

name_table = 'orders'
name_file = 'north_data/orders_data.csv'

fill_postgres_from_csv(password, name_table, name_file)
