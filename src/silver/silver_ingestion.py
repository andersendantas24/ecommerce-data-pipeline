from pyspark.sql.functions import col, to_timestamp
from pathlib import Path




def convert_to_timestamp(df, columns):
    
    for column in columns:
        df = df.withColumn(column, to_timestamp(col(column)))
    
    return df



def get_date_columns(df):
    return [
        c for c in df.columns
        if "date" in c.lower() or "timestamp" in c.lower()
    ]



def remove_null(df, required_columns):
    colunas_existentes = [
        c for c in required_columns
        if c in df.columns
    ]

    if not colunas_existentes:
        print("Nenhuma coluna válida para remoção de nulos.")
        return df

    before = df.count()

    df = df.dropna(subset=colunas_existentes)

    after = df.count()

    print(f"Colunas usadas para remoção: {colunas_existentes}")
    print(f"Registros removidos: {before - after}")
    print(f"Total restante: {after}")

    return df



def remove_duplicates(df, subset_columns):
    
    colunas_existentes = [
        c for c in subset_columns
        if c in df.columns
    ]

    if not colunas_existentes:
        print("Nenhuma coluna válida para deduplicação.")
        return df

    before = df.count()

    df = df.dropDuplicates(colunas_existentes)

    after = df.count()

    print(f"Colunas usadas para deduplicação: {colunas_existentes}")
    print(f"Duplicados removidos: {before - after}")
    print(f"Total restante: {after}")

    return df



def process_table(spark, bronze_path, silver_path):
    
    df = spark.read.format("delta").load(bronze_path)

    # identifica colunas de data automaticamente
    date_columns = get_date_columns(df)

    print(f"Convertendo colunas: {date_columns}")

    df = convert_to_timestamp(df, date_columns)

    # remove registros com chaves nulas, quando existirem na tabela
    df = remove_null(df, ["order_id", "customer_id"])

    # 🔥 remover duplicados
    df = remove_duplicates(df, ["order_id"])

    # salva na Silver
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(silver_path)
    


def silver_ingestion(spark):

    BASE_DIR = Path(__file__).resolve().parents[2]
    BRONZE_PATH = BASE_DIR / "delta" / "bronze"
    SILVER_PATH = BASE_DIR / "delta" / "silver"

    for table_path in BRONZE_PATH.iterdir():

        if table_path.is_dir():

            bronze_table = str(table_path)
            silver_table = str(SILVER_PATH / table_path.name)

            print(f"Processando: {table_path.name}")

            process_table(spark, bronze_table, silver_table)



def validate_silver_tables(spark):
    BASE_DIR = Path(__file__).resolve().parents[2]
    SILVER_PATH = BASE_DIR / "delta" / "silver"

    for table_path in SILVER_PATH.iterdir():

        if table_path.is_dir():
            print("=" * 80)
            print(f"Tabela Silver: {table_path.name}")
            print("=" * 80)

            df = spark.read.format("delta").load(str(table_path))

            print("Schema:")
            df.printSchema()

            print("Amostra de 10 linhas:")
            df.show(10, truncate=False)



def debug_silver_tables(spark):
    BASE_DIR = Path(__file__).resolve().parents[2]
    BRONZE_PATH = BASE_DIR / "delta" / "bronze"

    for table_path in BRONZE_PATH.iterdir():

        if table_path.is_dir():
            print("=" * 80)
            print(f"Testando tabela: {table_path.name}")
            print("=" * 80)

            try:
                df = spark.read.format("delta").load(str(table_path))

                date_columns = get_date_columns(df)
                print(f"Colunas detectadas: {date_columns}")

                df = convert_to_timestamp(df, date_columns)

                df.limit(10).show()

                print("✅ OK")

            except Exception as e:
                print(f"❌ ERRO na tabela: {table_path.name}")
                print(e)