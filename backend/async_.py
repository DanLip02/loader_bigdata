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
path = os.getenv("path")

DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"
CSV_FILE_PATH = path

TABLE_NAME = table
SCHEMA = schema

DTYPE_MAP, ORDER_ = data_base.table_info(user, password, host, port, db, table)

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
    ddf = ddf[ORDER_]
    print(len(ddf.columns))
    columns = ", ".join(ORDER_)
    placeholders = ", ".join([f"${i+1}" for i in range(len(ORDER_))])
    insert_query = f"INSERT INTO {SCHEMA}.{TABLE_NAME} ({columns}) VALUES ({placeholders})"

    counter = 0
    for batch_df in ddf.to_delayed():
        counter += 1
        print(f"🚀 Загружаем batch #{counter}...")

        batch = await asyncio.to_thread(lambda: batch_df.compute())

        batch = batch.where(pd.notna(batch), 'null')

        records = [tuple(row) for row in batch.itertuples(index=False, name=None)]

        # push Postgresql

        await conn.executemany(insert_query, records)
        # if counter == 46:
        #     print(f"📌 Проверяем batch #{counter}")
        #
        #
        #     batch = batch_df.compute()
        #
        #
        #     print(batch.head())
        #
        #
        #     print("Число столбцов в строках:", batch.apply(lambda row: len(row), axis=1).unique())
        #     stop = 0
        #

    await conn.close()
    print("✅ Данные успешно загружены!")

# start
asyncio.run(load_csv_to_postgres())