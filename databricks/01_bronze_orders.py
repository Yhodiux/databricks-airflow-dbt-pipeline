# Databricks notebook source
df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation","/Volumes/workspace/bronze/orders_files/_schema")
    .load("/Volumes/workspace/bronze/orders_files/")
)

from pyspark.sql.functions import col

df_raw = (
    df_raw
    .select(
        col("order_id").cast("long").alias("order_id"),
        col("customer_id").cast("long").alias("customer_id"),
        col("amount").cast("string").alias("amount"),
        col("status"),
        col("updated_at").cast("string").alias("updated_at")
    )
)

query = (
    df_raw.writeStream
    .format("delta")
    .option(
        "checkpointLocation",
        "/Volumes/workspace/bronze/orders_files/_checkpoint"
    )
    .trigger(availableNow=True)
    .toTable("workspace.bronze.orders")
)

query.awaitTermination()

# COMMAND ----------

display(
    spark.table("workspace.bronze.orders")
    .filter("order_id >= 110")
    .orderBy("order_id")
)