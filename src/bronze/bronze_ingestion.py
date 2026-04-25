

from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp





def get_spark(app_name: str = "ecommerce"):
    builder = (
        SparkSession.builder
        .appName('ecommerce')
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # .config("spark.hadoop.io.nativeio", "false")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()



def read_csv(file_path: Path):
    spark = get_spark()

    return(
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(file_path))
    )

def add_ingestion_timestamp(df):
    return df.withColumn("ingestion_timestamp", current_timestamp())


def ingest_raw_to_bronze():

    BASE_DIR = Path(__file__).resolve().parents[2]
    RAW_PATH = BASE_DIR /"data" / "raw"
    BRONZE_PATH = BASE_DIR /"delta" / "bronze"

    for file in RAW_PATH.glob("*.csv"):
        df = read_csv(file)

        df = add_ingestion_timestamp(df)

        output_path = BRONZE_PATH / file.stem

        df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(output_path))


        print(f"Processando: {file.name}")
    