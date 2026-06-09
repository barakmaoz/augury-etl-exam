import io
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

# Lazily create the connection pool on first use so importing this module does not require a
# live database (lets pure unit tests import the app, and DB tests skip cleanly when no DB).
pool_augury_db = None


def _get_pool() -> SimpleConnectionPool:
    global pool_augury_db
    if pool_augury_db is None:
        pool_augury_db = SimpleConnectionPool(
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASS"),
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT"),
            maxconn=10,
            minconn=1,
        )
    return pool_augury_db


from contextlib import contextmanager

@contextmanager
def get_connection(pool_conn:SimpleConnectionPool):
    connection = pool_conn.getconn()
    try:
        yield connection
    finally:
        pool_conn.putconn(connection)

@contextmanager
def get_curser(connection):
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
         yield cursor

def create_chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]



class PostgresConnector:
    
    @staticmethod
    def execute_query(query:str, return_df:bool=False,binds=None) -> pd.DataFrame :
        with get_connection(_get_pool()) as connection:
            with get_curser(connection) as cursor:
                cursor.execute(query,binds)
                if return_df:
                    result = cursor.fetchall()
                    return pd.DataFrame(result)
            connection.commit()

    @staticmethod
    def insert_query_with_copy(connection,schema, table, df: pd.DataFrame):
        columns = df.columns.tolist()
        sql = f"""
            COPY {schema}.{table} ({','.join(columns)}) 
            FROM STDIN 
            WITH CSV DELIMITER AS ','
            """

        print(f"Inserting {len(df)} rows into {schema}.{table}...")
        # print(sql)
        csv_buffer = io.StringIO(df.to_csv(index=False, header=False))
        with get_curser(connection) as cursor:
            cursor.copy_expert(sql, csv_buffer)
            connection.commit()
    
    @classmethod
    def upsert_query(cls,schema, table, df: pd.DataFrame, unique_columns):
        for col in unique_columns:
            if col not in df.columns:
                raise ValueError(
                    f"Unique key column {col} not found in dataframe")

        df_chunks = create_chunks(df, 5000)
        for df_chunk in df_chunks:
            delete_query = f"DELETE FROM {schema}.{table} WHERE ({','.join(unique_columns)}) IN ({','.join(['%s']*len(df_chunk))})"

            with get_connection(_get_pool()) as connection:
                with get_curser(connection) as cursor:
                    cursor.execute(delete_query, df_chunk[unique_columns[0]].tolist() if len(unique_columns) == 1 else list(
                        df_chunk[unique_columns].itertuples(index=False, name=None)))
            
                cls.insert_query_with_copy(connection,schema, table, df_chunk)

    @classmethod
    def insert_query(cls,schema, table, df: pd.DataFrame):

        df_chunks = create_chunks(df, 5000)
        for df_chunk in df_chunks:

            with get_connection(_get_pool()) as connection:
                cls.insert_query_with_copy(connection,schema, table, df_chunk)
