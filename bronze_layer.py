from pyspark import pipelines as dp
from pyspark.sql import functions as f

# Ścieżka do kontenera ADLS Gen2 w Azure (zamiast lokalnego Volume)
base_path = "abfss://retailflow@retailflow21102026.dfs.core.windows.net/landing/"

# Materialized views (Wymiary)

@dp.table(name="bronze_layer.bronze_stores")
def bronze_stores():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .load(f"{base_path}stores")
    )

@dp.table(name="bronze_layer.bronze_products")
def bronze_products():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .load(f"{base_path}products")
    )

@dp.table(name="bronze_layer.bronze_customers")
def bronze_customers():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .load(f"{base_path}customers")
    )

# Streaming tables (Transakcje - Auto Loader)

@dp.table(name="bronze_layer.bronze_order_items")
def bronze_order_items():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{base_path}order_items")
        .withColumn("ingested_at", f.current_timestamp())
    )

@dp.table(name="bronze_layer.bronze_orders")
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{base_path}orders")
        .withColumn("ingested_at", f.current_timestamp())
    )

@dp.table(name="bronze_layer.bronze_returns")
def bronze_returns():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{base_path}returns")
        .withColumn("ingested_at", f.current_timestamp())
    )