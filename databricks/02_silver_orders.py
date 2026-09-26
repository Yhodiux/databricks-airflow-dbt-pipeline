# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Leer Bronze
df_bronze = spark.table("workspace.bronze.orders")

# Tipificar
df_clean = (
    df_bronze
    .withColumn("amount", col("amount").cast("decimal(10,2)"))
    .withColumn("updated_at", to_timestamp(col("updated_at")))
)

# Conservar el registro más reciente de cada order_id
window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(col("updated_at").desc())
)

df_clean = (
    df_clean
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# MERGE contra Silver
silver = DeltaTable.forName(
    spark,
    "workspace.silver.orders"
)

(
    silver.alias("target")
    .merge(
        df_clean.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)