import requests
import pandas as pd
import psycopg2
import json
import unicodedata
from dotenv import load_dotenv
import os
from datetime import datetime
import time
load_dotenv()

# Configuração da API
url = "https://api2.ploomes.com/Deals?$expand=OtherProperties"
user_key = os.getenv("user_key")
headers = {'User-Key': user_key, 'Content-Type': 'application/json'}

# Configuração do banco PostgreSQL
db_config = {
    "dbname": "Ploomes",
    "user": "postgres",
    "password": os.getenv('DB_PASSWORD'),
    "host": "localhost",
    "port": "5432"
}

# Mapeamento de campos personalizados
custom_columns = {
    'Área de Projetos': 42716145,
    'Temperatura': 42716097,
    'Data Apresentação de Proposta': 42765193,
    'Data Assinatura do Contrato': 42716366,
    'Previsão de Fechamento': 42716095,
    'Entrada da Proposta': 42824426,
    'Valor Mensal da Proposta': 42824429
}

# Função para mapear tipos de dados do pandas para PostgreSQL
def map_pandas_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

# Função para sanitizar nomes de colunas
def sanitize_column_name(name):
    name = ''.join(c for c in unicodedata.normalize('NFD', str(name)) 
                 if unicodedata.category(c) != 'Mn')
    return name.replace(' ', '_').replace('-', '_').replace('.', '_').lower()

# Função para extrair valores de campos personalizados
def extrair_valor_por_id(other_properties, field_id):
    if not isinstance(other_properties, list):
        return None
    for campo in other_properties:
        if campo.get('FieldId') == field_id:
            return (
                campo.get('StringValue') or
                campo.get('ObjectValueName') or
                campo.get('UserValueName') or
                campo.get('ContactValueName') or
                campo.get('DateTimeValue') or
                campo.get('DecimalValue')
            )
    return None

# Função para buscar dados da API
def fetch_data():
    skip = 0
    batch_size = 300
    all_deals = []

    try:
        while True:
            paginated_url = f"{url}&$skip={skip}"
            response = requests.get(paginated_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                deals = data.get('value', [])
                if not deals:
                    break
                all_deals.extend(deals)
                skip += batch_size
            elif response.status_code == 429:
                print("Erro 429: Muitas requisições. Aguarde...")
                time.sleep(60)
                continue
            else:
                print(f"Erro {response.status_code}: {response.text}")
                break
        return all_deals
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a API: {e}")
        return []

# Função para transformar datas
'''def transform_dates(df, date_columns):
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], errors='coerce').dt.tz_localize(None)
    return df'''

# Função para criar tabela no PostgreSQL
def create_postgres_table(df, table_name, db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Gera definições de colunas
        col_definitions = []
        for col in df.columns:
            pg_type = map_pandas_to_postgres(df[col])
            col_definitions.append(f"{sanitize_column_name(col)} {pg_type}")
        
        # Cria a tabela
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        create_sql = f"""
        CREATE TABLE {table_name} (
            {', '.join(col_definitions)},
            PRIMARY KEY (id)
        )
        """
        cursor.execute(create_sql)
        conn.commit()
        print(f"Tabela '{table_name}' criada com sucesso!")
        return True
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Função para inserir dados no PostgreSQL
def insert_to_postgres(df, table_name, db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Prepara os dados
        df = df.where(pd.notnull(df), None)
        columns = [sanitize_column_name(col) for col in df.columns]
        
        # Cria string SQL
        insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s']*len(columns))})
        ON CONFLICT (id) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])}
        """
        
        # Converte DataFrame para lista de tuplas
        data = [tuple(x) for x in df.to_numpy()]
        
        # Executa em lotes para melhor performance
        batch_size = 100
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            print(f"Inseridos {len(batch)} registros (total: {min(i + batch_size, len(data))}/{len(data)})")
        
        print(f"Total de {len(data)} registros inseridos/atualizados com sucesso!")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Erro ao inserir dados: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Fluxo principal
if __name__ == "__main__":
    # 1. Extrair dados da API
    print("Iniciando extração de dados da API Ploomes...")
    dados = fetch_data()
    
    if not dados:
        print("Nenhum dado retornado pela API.")
        exit()
    
    print(f"Total de registros obtidos: {len(dados)}")
    
    # 2. Transformar dados em DataFrame
    df = pd.json_normalize(dados, sep='_')
    
    # 3. Adicionar colunas personalizadas
    for col_name, field_id in custom_columns.items():
        df[col_name] = df['OtherProperties'].apply(lambda x: extrair_valor_por_id(x, field_id))
    
    # 4. Remover coluna OtherProperties
    df.drop(columns=['OtherProperties'], inplace=True)
    
    # 5. Transformar datas
    date_columns = [
        'StartDate',
        'FinishDate',
        'Data Apresentação de Proposta',
        'Data Assinatura do Contrato',
        'Previsão de Fechamento',
        'CreateDate',
        'LastUpdateDate'
    ]
    #df = transform_dates(df, date_columns)
    # 6. Sanitizar nomes das colunas
    df.columns = [sanitize_column_name(col) for col in df.columns]
    
    # 7. Conectar ao PostgreSQL e carregar dados
    table_name = "fnegociacoes"
    
    if create_postgres_table(df, table_name, db_config):
        insert_to_postgres(df, table_name, db_config)
    
    print("Processo concluído!")