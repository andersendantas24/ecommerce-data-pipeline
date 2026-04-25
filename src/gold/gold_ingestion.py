from pathlib import Path
from pyspark.sql.functions import count, col, countDistinct, sum as spark_sum, avg


def create_gold_customer_summary(spark):
    BASE_DIR = Path(__file__).resolve().parents[2]

    SILVER_PATH = BASE_DIR / "delta" / "silver"
    GOLD_PATH = BASE_DIR / "delta" / "gold"

    orders_df = spark.read.format("delta").load(
        str(SILVER_PATH / "orders_consolidated")
    )

    reviews_df = spark.read.format("delta").load(
        str(SILVER_PATH / "olist_order_reviews_dataset")
    )

    customer_summary_df = (
        orders_df
        .join(
            reviews_df.select("order_id", "review_score"),
            on="order_id",
            how="left"
        )
        .groupBy(
            "customer_id",
            "customer_city",
            "customer_state"
        )
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum(col("price") + col("freight_value")).alias("total_revenue"),
            (
                spark_sum(col("price") + col("freight_value")) 
                / countDistinct("order_id")
            ).alias("avg_order_value"),
            avg("review_score").alias("avg_review_score")
        )
    )

    customer_summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(GOLD_PATH / "customer_summary"))

    print("Tabela Gold customer_summary criada com sucesso.")



def create_gold_product_summary(spark):

    BASE_DIR = Path(__file__).resolve().parents[2]
    
    SILVER_PATH = BASE_DIR / "delta" / "silver"
    GOLD_PATH = BASE_DIR / "delta" / "gold"

    # leitura da tabela consolidada
    df = spark.read.format("delta").load(str(SILVER_PATH / "orders_consolidated"))

    # agregação
    product_summary = (
        df.groupBy("product_category_name")
        .agg(
            count("*").alias("total_units_sold"),
            spark_sum(col("price") + col("freight_value")).alias("total_revenue"),
            countDistinct("order_id").alias("total_orders"),
            avg("freight_value").alias("avg_freight_value")
        )
    )

    # salvar como Delta
    product_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(GOLD_PATH / "product_summary"))

    print("Tabela gold_product_summary criada com sucesso!")



def gold_ingestion(spark):
    BASE_DIR = Path(__file__).resolve().parents[2]
    GOLD_PATH = BASE_DIR / "delta" / "gold"
    
    steps = [
        ("customer_summary", create_gold_customer_summary),
        ("product_summary", create_gold_product_summary)
        # ("seller_summary", create_seller_summary),
    ]

    for name, func in steps:
        
        print(f"Criando {name}...")
        func(spark)
        print(f"{name} criado com sucesso!")
        df = spark.read.format("delta").load(str(GOLD_PATH / name))
        df.show(10, truncate=False)

        

    