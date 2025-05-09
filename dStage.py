import requests
import pandas as pd
import psycopg2
import json
import unicodedata
from dotenv import load_dotenv
import os

load_dotenv()

# Configuração da API
url = "https://api2.ploomes.com/Deals@Stages"
user_key = os.getenv('USER_KEY')
headers = {'User-Key': user_key, 'Content-Type': 'application/json'}

# Configuração do PostgreSQL
db_config = {
    "dbname": "Ploomes",
    "user": "postgres",
    "password": os.getenv('DB_PASSWORD'),
    "host": "localhost",
    "port": "5432"
}


def get_pg_connection():
    return psycopg2.connect(**db_config)

def table_exists(table_name):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = %s
            );
        """, (table_name.lower(),))
        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"Erro ao verificar tabela: {e}")
        return False

def map_pandas_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC(18,6)"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

def sanitize_column_name(name):
    name = ''.join(c for c in unicodedata.normalize('NFD', str(name)) if unicodedata.category(c) != 'Mn')
    return name.replace(' ', '_').replace('-', '_').replace('.', '_').lower()

def fetch_data():
    skip = 0
    batch_size = 300
    all_deals = []

    try:
        while True:
            response = requests.get(f"{url}?$skip={skip}", headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            deals = data.get('value', [])
            if not deals:
                break
            all_deals.extend(deals)
            skip += batch_size
        return all_deals
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a API: {e}")
        return []

def get_existing_ids(table_name):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT id FROM {table_name.lower()}")
        existing_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        return existing_ids
    except Exception as e:
        print(f"Erro ao buscar IDs existentes: {e}")
        return set()

def create_table_from_dataframe(df, table_name):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        
        # Sanitizar nomes de colunas
        df.columns = [sanitize_column_name(col) for col in df.columns]
        
        col_definitions = ", ".join([f"{col} {map_pandas_to_postgres(df[col])}" for col in df.columns])
        
        cursor.execute(f"""
            CREATE TABLE {table_name.lower()} (
                {col_definitions},
                PRIMARY KEY (id)
            )
        """)
        conn.commit()
        print(f"Tabela '{table_name.lower()}' criada com sucesso!")
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_new_data(df, table_name):
    conn = None
    cursor = None
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        
        table_name = table_name.lower()
        
        if not table_exists(table_name):
            create_table_from_dataframe(df, table_name)
        
        # Sanitizar nomes de colunas ANTES de filtrar
        df.columns = [sanitize_column_name(col) for col in df.columns]
        
        existing_ids = get_existing_ids(table_name)
        # Agora usamos 'id' em vez de 'Id'
        df_filtered = df[~df['id'].isin(existing_ids)]
        
        if df_filtered.empty:
            print("Nenhum novo dado para inserir.")
            return
        
        # Substituir NaN por None e ajustar floats
        df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
        for col in df_filtered.select_dtypes(include=['float']).columns:
            df_filtered[col] = df_filtered[col].apply(lambda x: round(x, 6) if x is not None else None)
        
        columns = ", ".join(col for col in df_filtered.columns)
        placeholders = ", ".join(["%s"] * len(df_filtered.columns))
        insert_sql = f"""
            INSERT INTO {table_name} ({columns}) 
            VALUES ({placeholders})
            ON CONFLICT (id) DO NOTHING
        """
        
        data_to_insert = [tuple(row) for row in df_filtered.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        print(f"{len(data_to_insert)} novos registros inseridos com sucesso!")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Erro ao inserir dados: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Fluxo principal
if __name__ == "__main__":
    dados = fetch_data()
    
    if dados:
        df = pd.json_normalize(dados, sep='_')
        table_name = "destagio"
        insert_new_data(df, table_name)
    else:
        print("Nenhum dado retornado pela API.")