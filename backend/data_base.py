import pandas as pd
from sqlalchemy import create_engine

def table_info(user, password, host, port, db, table):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table}';
        """

    df_types = pd.read_sql(query, engine)
    print(df_types)
    dtype_map = {
        "text": "string",
        "character varying": "str",
        "real": "float",
        "integer": "int64",
        "bigint": "int64",
        "smallint": "int64",
        "boolean": "bool",
        "timestamp without time zone": "datetime64[ns]",
        "timestamp with time zone": "datetime64[ns]",
        "date": "datetime64[ns]"
    }

    df_types["data_type"] = df_types["data_type"].map(dtype_map)
    # dask_dtypes = {row["column_name"]: dtype_map.get(row["data_type"], "object") for _, row in df_types.iterrows()}
    dask_dtypes = list(df_types.set_index("column_name").to_dict().values())[0]
    dask_column_order = df_types["column_name"].values
    return dask_dtypes, dask_column_order

def write_partition(partition):
    from dotenv import load_dotenv
    import os

    load_dotenv()
    user = os.getenv('user')
    password = os.getenv('password')
    port = os.getenv('port')
    host = os.getenv('host')
    db = os.getenv('db')
    table = os.getenv('table')
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}", echo=True)

    partition.fillna('null').to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=100000)
