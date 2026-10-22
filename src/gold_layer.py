from pyspark import pipelines as dp
from pyspark.sql import functions as f
from pyspark.sql.window import Window



# Model 1: gold_monthly_channel_gmv (Pytanie 1: GMV i średnia wartość zamówienia per kanał i miesiąc)
@dp.table(
    name="gold_layer.gold_monthly_channel_gmv",
    schema="""
        year_month STRING,
        channel STRING,
        total_gmv DECIMAL(12, 2),
        avg_order_value DECIMAL(10, 2),
        order_count INT,
        unique_customers INT
    """
)
def gold_monthly_channel_gmv():
    df = spark.read.table("silver_layer.silver_orders")

    return (
        df.withColumn("year_month", f.date_format(f.col("order_date"), "yyyy-MM"))
          .groupBy("year_month", "channel")
          .agg(
              f.sum("total_amount").cast("decimal(12, 2)").alias("total_gmv"),
              f.round(f.avg("total_amount"), 2).cast("decimal(10, 2)").alias("avg_order_value"),
              f.count("order_id").cast("int").alias("order_count"),
              f.countDistinct("customer_id").cast("int").alias("unique_customers")
          )
          .orderBy("year_month", "channel")
    )


# Model 2: gold_customer_segments_revenue (Pytanie 2: Przychody klientów i analiza per segment)
@dp.table(
    name="gold_layer.gold_customer_segments_revenue",
    schema="""
        customer_id INT,
        full_name STRING,
        city STRING,
        segment STRING,
        customer_lifetime_value DECIMAL(12, 2),
        total_orders INT,
        first_order_date DATE,
        last_order_date DATE
    """
)
def gold_customer_segments_revenue():
    customers_df = spark.read.table("silver_layer.silver_customers")
    orders_df = spark.read.table("silver_layer.silver_orders")

    customer_orders = (
        orders_df.groupBy("customer_id")
        .agg(
            f.sum("total_amount").cast("decimal(12, 2)").alias("customer_lifetime_value"),
            f.count("order_id").cast("int").alias("total_orders"),
            f.min("order_date").alias("first_order_date"),
            f.max("order_date").alias("last_order_date")
        )
    )

    return (
        customers_df.join(customer_orders, "customer_id", "inner")
        .withColumn(
            "full_name",
            f.concat_ws(" ", f.col("first_name"), f.col("last_name"))
        )
        .select(
            "customer_id",
            "full_name",
            "city",
            "segment",
            "customer_lifetime_value",
            "total_orders",
            "first_order_date",
            "last_order_date"
        )
        .orderBy(f.col("customer_lifetime_value").desc())
    )


# Model 3: gold_product_returns_analysis (Pytanie 3: Produkty z największą liczbą zwrotów i stopa zwrotu per kategoria)
@dp.table(
    name="gold_layer.gold_product_returns_analysis",
    schema="""
        product_id INT,
        product_name STRING,
        category STRING,
        brand STRING,
        total_sold_units INT,
        total_returned_units INT,
        product_return_rate_pct DECIMAL(5, 2),
        category_return_rate_pct DECIMAL(5, 2)
    """
)
def gold_product_returns_analysis():
    order_items_df = spark.read.table("silver_layer.silver_order_items")
    products_df = spark.read.table("silver_layer.silver_products")

    category_window = Window.partitionBy("category")

    item_stats = (
        order_items_df.groupBy("product_id")
        .agg(
            f.sum("quantity").cast("int").alias("total_sold_units"),
            f.sum(
                f.when(f.col("returned") == True, f.col("quantity")).otherwise(0)
            ).cast("int").alias("total_returned_units")
        )
    )

    product_stats = item_stats.withColumn(
        "product_return_rate_pct",
        f.when(
            f.col("total_sold_units") > 0,
            f.round((f.col("total_returned_units") / f.col("total_sold_units")) * 100, 2)
        ).otherwise(f.lit(0.00)).cast("decimal(5, 2)")
    )

    return (
        products_df.join(product_stats, "product_id", "inner")
        .withColumn(
            "category_return_rate_pct",
            f.round(
                (f.sum("total_returned_units").over(category_window) /
                 f.sum("total_sold_units").over(category_window)) * 100, 2
            ).cast("decimal(5, 2)")
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "brand",
            "total_sold_units",
            "total_returned_units",
            "product_return_rate_pct",
            "category_return_rate_pct"
        )
        .orderBy(f.col("total_returned_units").desc())
    )


# Model 4: gold_store_performance (Pytanie 5: Sklepy stacjonarne vs Sklep Online)
@dp.table(
    name="gold_layer.gold_store_performance",
    schema="""
        store_id INT,
        store_name STRING,
        store_type STRING,
        channel_category STRING,
        total_revenue DECIMAL(12, 2),
        order_count INT,
        revenue_share_pct DECIMAL(5, 2)
    """
)
def gold_store_performance():
    orders_df = spark.read.table("silver_layer.silver_orders")
    stores_df = spark.read.table("silver_layer.silver_stores")

    total_window = Window.partitionBy()

    store_revenue = (
        orders_df.groupBy("store_id")
        .agg(
            f.sum("total_amount").cast("decimal(12, 2)").alias("total_revenue"),
            f.count("order_id").cast("int").alias("order_count")
        )
    )

    joined_df = (
        stores_df.join(store_revenue, "store_id", "inner")
        .withColumn(
            "channel_category",
            f.when(
                f.lower(f.col("store_type")).contains("online") | (f.col("store_id") == 105),
                f.lit("Online")
            ).otherwise(f.lit("Stacjonarny"))
        )
        .withColumn(
            "revenue_share_pct",
            f.round(
                (f.col("total_revenue") / f.sum("total_revenue").over(total_window)) * 100, 2
            ).cast("decimal(5, 2)")
        )
    )

    return (
        joined_df.select(
            "store_id",
            "store_name",
            "store_type",
            "channel_category",
            "total_revenue",
            "order_count",
            "revenue_share_pct"
        )
        .orderBy(f.col("total_revenue").desc())
    )


# Model 5: gold_returns_breakdown (Pytanie 6: Powody zwrotów i łączna wartość zwróconych środków)
@dp.table(
    name="gold_layer.gold_returns_breakdown",
    schema="""
        reason STRING,
        return_count INT,
        return_share_pct DECIMAL(5, 2),
        total_refunded_amount DECIMAL(12, 2),
        avg_refund_amount DECIMAL(10, 2)
    """
)
def gold_returns_breakdown():
    returns_df = spark.read.table("silver_layer.silver_returns")

    total_window = Window.partitionBy()

    return (
        returns_df.groupBy("reason")
        .agg(
            f.count("return_id").cast("int").alias("return_count"),
            f.sum("refund_amount").cast("decimal(12, 2)").alias("total_refunded_amount"),
            f.round(f.avg("refund_amount"), 2).cast("decimal(10, 2)").alias("avg_refund_amount")
        )
        .withColumn(
            "return_share_pct",
            f.round(
                (f.col("return_count") / f.sum("return_count").over(total_window)) * 100, 2
            ).cast("decimal(5, 2)")
        )
        .select(
            "reason",
            "return_count",
            "return_share_pct",
            "total_refunded_amount",
            "avg_refund_amount"
        )
        .orderBy(f.col("return_count").desc())
    )


