# Databricks notebook source
from pyspark.sql.functions import col, count, sum

df_gold = (
    spark.table("workspace.silver.orders")
    .filter(col("status") == "COMPLETED")
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_amount")
    )
)

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.customer_sales")