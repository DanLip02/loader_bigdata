import asyncio
import asyncpg
import dask.dataframe as dd
import pandas as pd
import os
from dotenv import load_dotenv
import data_base
import numpy as np

load_dotenv()
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")
host = os.getenv("host")
db = os.getenv("db")
table = os.getenv("table")
schema = os.getenv("schema")

DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"
CSV_FILE_PATH = r"C:\Users\danila.lipatov\PycharmProjects\1.csv"  # Убрал пробел

# Имя таблицы в PostgreSQL
TABLE_NAME = table
SCHEMA = schema

# Явное указание типов
DTYPE_MAP, dask_column_order = data_base.table_info(user, password, host, port, db, table)

async def load_csv_to_postgres():
    print("🔄 Подключаемся к базе данных...")
    conn = await asyncpg.connect(DATABASE_URL)

    print("📂 Загружаем CSV в Dask DataFrame...")
    ddf = dd.read_csv(CSV_FILE_PATH, dtype=DTYPE_MAP)
    ddf = ddf.repartition(npartitions=200)

    missing_cols = {col: dtype for col, dtype in DTYPE_MAP.items() if col not in ddf.columns}
    print(missing_cols)
    for col, dtype in missing_cols.items():
        if "int" in dtype or "float" in dtype:
            ddf[col] = np.nan
        elif "datetime" in dtype:
            ddf[col] = pd.NaT
        else:
            ddf[col] = None

    ddf = ddf.astype(DTYPE_MAP)
    ddf = ddf[dask_column_order]
    print(len(ddf.columns))
    columns = ", ".join(dask_column_order)
    placeholders = ", ".join([f"${i+1}" for i in range(len(dask_column_order))])
    insert_query = f"INSERT INTO {SCHEMA}.{TABLE_NAME} ({columns}) VALUES ({placeholders})"

    # Загружаем данные в PostgreSQL
    counter = 0
    for batch_df in ddf.to_delayed():
        counter += 1
        print(f"🚀 Загружаем batch #{counter}...")
        #
        # # Вычисляем данные в фоне
        # batch = await asyncio.to_thread(lambda: batch_df.compute())
        #
        # print("stop_2")
        # # Заменяем NaN/NaT на None
        # batch = batch.where(pd.notna(batch), 'null')
        #
        # # Приводим данные к списку кортежей
        # records = [tuple(row) for row in batch.itertuples(index=False, name=None)]
        #
        # # Вставляем данные в PostgreSQL
        # await conn.executemany(insert_query, records)
        if counter == 46:  # Пропускаем первые 45 батчей
            print(f"📌 Проверяем batch #{counter}")

            # Вычисляем данные
            batch = batch_df.compute()

            # Выводим 5 первых строк для анализа
            print(batch.head())

            # Проверяем число столбцов в каждой строке
            print("Число столбцов в строках:", batch.apply(lambda row: len(row), axis=1).unique())
            stop = 0


    # Закрываем соединение
    await conn.close()
    print("✅ Данные успешно загружены!")

# Запуск
asyncio.run(load_csv_to_postgres())
