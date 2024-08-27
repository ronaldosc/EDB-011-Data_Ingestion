#!pip install pyspark
#!pip install mysql

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import mysql.connector
import os
import time

# Configurações do MySQL
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'A170498b!',
    'database': 'grupo7',
    'port': 3306
}

# Criando a sessão Spark
spark = SparkSession.builder \
    .appName("DataProcessingPipeline") \
    .getOrCreate()

# Função para limpar e padronizar os dados
def clean_data(df):
    # Remover duplicatas
    df = df.dropDuplicates()

    # Substituir valores nulos por 0
    df = df.fillna(0)

    # Padronizar os nomes das colunas para lowercase e remover caracteres especiais
    for col_name in df.columns:
        new_col_name = col_name.lower()
        new_col_name = new_col_name.replace(" ", "_")  # Substituir espaços por underscore
        new_col_name = regexp_replace(new_col_name, "[^a-zA-Z0-9_]", "")  # Remover caracteres especiais
        df = df.withColumnRenamed(col_name, new_col_name)

    return df

# Função para garantir unicidade dos nomes das colunas
def ensure_unique_columns(columns):
    mapping = {}
    result = []
    for col in columns:
        col_normalized = normalize_column_name(col)
        if col_normalized in mapping:
            mapping[col_normalized] += 1
            unique_col = f"{col_normalized}_{mapping[col_normalized]}"
            result.append(unique_col[:64])  # Limitar a 64 caracteres
        else:
            mapping[col_normalized] = 0
            result.append(col_normalized)
    return result

# Função para normalizar nomes de colunas
def normalize_column_name(name):
    normalized = (
        name.strip()
            .lower()
            .replace(' ', '_')
            .replace(';', '_')
            .replace('.', '_')
            .replace('-', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('ç', 'c')
            .replace('ã', 'a')
            .replace('á', 'a')
            .replace('é', 'e')
            .replace('í', 'i')
            .replace('ó', 'o')
            .replace('ú', 'u')
            .replace('â', 'a')
            .replace('ê', 'e')
            .replace('ô', 'o')
            .replace(':', '_')
            .replace('__', '_')
    )
    return normalized[:64]  # Limitar a 64 caracteres

# Função para adicionar colunas que não existem na tabela
def add_missing_columns(cursor, table, columns):
    cursor.execute(f"DESCRIBE {table}")
    existing_columns = {row[0] for row in cursor.fetchall()}
    new_columns = set(columns) - existing_columns
    for col in new_columns:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN `{col}` TEXT")

# Tentar conectar ao MySQL com várias tentativas
max_retries = 5
retry_count = 0
while retry_count < max_retries:
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        break
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        print(f"Retrying ({retry_count+1}/{max_retries})...")
        retry_count += 1
        time.sleep(10)
else:
    raise Exception("Failed to connect to MySQL after several attempts")

# Função para salvar a camada Delivery no banco de dados relacional
def save_delivery_to_mysql(df, table_name, cursor):
    pandas_df = df.toPandas()
    
    # Normalizar e garantir nomes únicos das colunas
    pandas_df.columns = ensure_unique_columns(pandas_df.columns)
    
    # Criar a tabela no MySQL com base nas colunas normalizadas
    table_creation_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY)"
    cursor.execute(table_creation_query)
    
    # Adicionar colunas que não existem na tabela
    add_missing_columns(cursor, table_name, pandas_df.columns)
    
    # Inserir os dados na tabela
    for _, row in pandas_df.iterrows():
        columns = ", ".join([f"`{col}`" for col in pandas_df.columns])
        values = ", ".join(["%s"] * len(pandas_df.columns))
        insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({values})"
        cursor.execute(insert_query, tuple(row))

# Lista de arquivos e seus caminhos de saída
file_paths = [
    "/app/etl_project/csv_files/data/2021_tri_01.csv",
    "/app/etl_project/csv_files/2021_tri_02.csv",
    "/app/etl_project/csv_files/021_tri_03.csv",
    "/app/etl_project/csv_files/021_tri_04.csv",
    "/app/etl_project/csv_files/2022_tri_01.csv",
    "/app/etl_project/csv_files/2022_tri_03.csv",
    "/app/etl_project/csv_files/2022_tri_04.csv",
    "/app/etl_project/csv_files/data/EnquadramentoInicia_v2.csv",
    "/app/etl_project/csv_files/glassdoor_consolidado_join_match_less_v2.csv",
    "/app/etl_project/csv_files/glassdoor_consolidado_join_match_v2.csv"
]

# Aplicar a função de limpeza e salvar nas camadas RAW, Trusted e Delivery
for file_path in file_paths:
    # Leitura do arquivo RAW
    df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Processar a camada Trusted
    df_trusted = clean_data(df_raw)
    
    # Salvar a camada Delivery no banco de dados MySQL
    delivery_table_name = normalize_column_name(os.path.splitext(os.path.basename(file_path))[0])
    save_delivery_to_mysql(df_trusted, delivery_table_name, cursor)

# Criar a tabela 'tabela_fato' que combina todas as tabelas criadas
fato_table_creation_query = "CREATE TABLE IF NOT EXISTS `tabela_fato` (id INT AUTO_INCREMENT PRIMARY KEY)"
cursor.execute(fato_table_creation_query)

# Adicionar colunas à tabela 'tabela_fato'
all_columns = set()
for file_path in file_paths:
    table_name = normalize_column_name(os.path.splitext(os.path.basename(file_path))[0])
    cursor.execute(f"DESCRIBE `{table_name}`")
    table_columns = {row[0] for row in cursor.fetchall()}
    all_columns.update(table_columns)

all_columns.discard('id')  # Remover a coluna 'id' duplicada

# Adicionar colunas que não existem na tabela 'tabela_fato'
add_missing_columns(cursor, 'tabela_fato', all_columns)

# Inserir dados de todas as tabelas na tabela 'tabela_fato'
for file_path in file_paths:
    table_name = normalize_column_name(os.path.splitext(os.path.basename(file_path))[0])
    cursor.execute(f"DESCRIBE `{table_name}`")
    existing_columns = {row[0] for row in cursor.fetchall()}
    select_columns = ", ".join([f"`{col}`" for col in existing_columns if col in all_columns])
    insert_query = f"INSERT INTO `tabela_fato` ({select_columns}) SELECT {select_columns} FROM `{table_name}`"
    cursor.execute(insert_query)

# Confirmar transações e fechar conexão
conn.commit()
cursor.close()
conn.close()

# Finalizar a sessão Spark
spark.stop()
