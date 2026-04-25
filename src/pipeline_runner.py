from src.bronze.bronze_ingestion import ingest_raw_to_bronze, get_spark
from src.silver.silver_ingestion import silver_ingestion, validate_silver_tables, debug_silver_tables, create_payments_summary

# if __name__ == "__main__":
#     ingest_raw_to_bronze()


if __name__ == "__main__":
    spark = get_spark()

    # print("=== BRONZE ===")
    # ingest_raw_to_bronze()

    print("=== SILVER ===")
    silver_ingestion(spark)

    print("=== PAYMENTS ===")
    create_payments_summary(spark)

    # print("=== VALIDACAO ===")
    # validate_silver_tables(spark)

    # print("=== DEBUG ===")
    # debug_silver_tables(spark)

    spark.stop()
    print("Spark Encerrado")