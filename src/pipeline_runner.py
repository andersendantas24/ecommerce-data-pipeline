from datetime import datetime

from src.bronze.bronze_ingestion import ingest_raw_to_bronze, get_spark
from src.silver.silver_ingestion import silver_ingestion, create_payments_summary
from src.gold.gold_ingestion import gold_ingestion
from src.share_simulation.share_simulation import share_simulation


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def run_step(step_name, function, spark):
    log(f"Iniciando etapa: {step_name}")

    try:
        function(spark)
        log(f"Etapa finalizada com sucesso: {step_name}")

    except Exception as e:
        log(f"Erro na etapa {step_name}: {e}")

    print("-" * 80)


def run_pipeline():
    spark = get_spark()

    steps = [
        ("BRONZE", ingest_raw_to_bronze),
        ("SILVER", silver_ingestion),
        ("PAYMENTS", create_payments_summary),
        ("GOLD", gold_ingestion),
        ("SHARE_SIMULATION", share_simulation)
    ]

    log("Iniciando pipeline completo")

    for step_name, function in steps:
        run_step(step_name, function, spark)

    log("Pipeline completo finalizado")

    spark.stop()


if __name__ == "__main__":
    run_pipeline()



#--------------------------------------------


# if __name__ == "__main__":
#     spark = get_spark()

    # print("=== BRONZE ===")
    # ingest_raw_to_bronze()

    # print("=== SILVER ===")
    # silver_ingestion(spark)
    # print("=== FIM SILVER ===")

    # print("=== PAYMENTS ===")
    # create_payments_summary(spark)

    # print("=== GOLD ===")
    # gold_ingestion(spark)
    # print("=== FIM GOLD ===")

    # print("=== Delta_Sharing ===")
    # share_simulation(spark)
    # print("=== FIM Delta_Sharing ===")

    # print("=== VALIDACAO ===")
    # validate_silver_tables(spark)

    # print("=== DEBUG ===")
    # debug_silver_tables(spark)

    # spark.stop()
    # print("Spark Encerrado")