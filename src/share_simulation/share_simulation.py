from pathlib import Path
import shutil
from pyspark.sql.functions import col, sum as spark_sum, desc


def export_single_csv(df, output_file: Path):
    temp_dir = output_file.parent / f"_tmp_{output_file.stem}"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    if output_file.exists():
        output_file.unlink()

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(str(temp_dir))

    part_file = next(temp_dir.glob("part-*.csv"))
    shutil.move(str(part_file), str(output_file))

    shutil.rmtree(temp_dir)


def read_gold_table(spark, gold_path: Path, table_name: str):
    return spark.read.format("delta").load(str(gold_path / table_name))


def share_simulation(spark):
    BASE_DIR = Path(__file__).resolve().parents[2]
    GOLD_PATH = BASE_DIR / "delta" / "gold"
    OUTPUT_PATH = BASE_DIR / "output"

    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    print("=== SIMULAÇÃO DELTA SHARING ===")

    customer_df = read_gold_table(spark, GOLD_PATH, "customer_summary")
    product_df = read_gold_table(spark, GOLD_PATH, "product_summary")
    seller_df = read_gold_table(spark, GOLD_PATH, "seller_summary")

    export_single_csv(
        customer_df,
        OUTPUT_PATH / "gold_customer_summary_export.csv"
    )

    export_single_csv(
        product_df,
        OUTPUT_PATH / "gold_product_summary_export.csv"
    )

    export_single_csv(
        seller_df,
        OUTPUT_PATH / "gold_seller_summary_export.csv"
    )

    print("Arquivos CSV exportados com sucesso em output/")

    total_clientes = customer_df.select("customer_id").distinct().count()

    receita_total = customer_df.agg(
        spark_sum("total_revenue").alias("receita_total")
    ).collect()[0]["receita_total"]

    estado_maior_pedidos = (
        customer_df
        .groupBy("customer_state")
        .agg(spark_sum("total_orders").alias("total_orders"))
        .orderBy(desc("total_orders"))
        .limit(1)
        .collect()[0]
    )

    top_3_categorias = (
        product_df
        .select("product_category_name", "total_revenue")
        .orderBy(desc("total_revenue"))
        .limit(3)
        .collect()
    )

    melhor_vendedor = (
        seller_df
        .filter(col("total_orders") >= 10)
        .orderBy(desc("avg_review_score"), desc("total_orders"))
        .limit(1)
        .collect()[0]
    )

    print("\n=== RESUMO EXECUTIVO ===")
    print(f"Total de clientes únicos atendidos: {total_clientes}")
    print(f"Receita total consolidada: R$ {receita_total:,.2f}")
    print(
        f"Estado com maior volume de pedidos: "
        f"{estado_maior_pedidos['customer_state']} "
        f"({estado_maior_pedidos['total_orders']} pedidos)"
    )

    print("\nTop 3 categorias de produto por receita:")
    for row in top_3_categorias:
        print(
            f"- {row['product_category_name']}: "
            f"R$ {row['total_revenue']:,.2f}"
        )

    print(
        "\nVendedor com melhor avaliação média "
        "(mínimo 10 pedidos):"
    )
    print(
        f"- Seller ID: {melhor_vendedor['seller_id']} | "
        f"Avaliação média: {melhor_vendedor['avg_review_score']:.2f} | "
        f"Pedidos: {melhor_vendedor['total_orders']}"
    )

    print("\n=== FIM SIMULAÇÃO DELTA SHARING ===")