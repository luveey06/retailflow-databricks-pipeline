from pyspark import pipelines as dp
from pyspark.sql import functions as f


def clean_empty_strings(col_name):
    trimmed_column = f.trim(f.col(col_name))
    cleaned_column = (
        f.when(f.lower(trimmed_column).isin("", "nan", "null", "none"), f.lit(None))
         .otherwise(trimmed_column)
    )
    return cleaned_column


def parse_boolean(col_name):
    value = f.lower(f.trim(f.col(col_name)))
    return (
        f.when(value.isin("true", "t", "1", "yes", "y"), f.lit(True))
         .when(value.isin("false", "f", "0", "no", "n"), f.lit(False))
         .otherwise(f.lit(None).cast("boolean"))
    )


def parse_date(col_name):
    cleaned = clean_empty_strings(col_name)
    return f.coalesce(
        f.try_to_date(cleaned, "yyyy-MM-dd"),
        f.try_to_date(cleaned, "dd.MM.yyyy"),
        f.try_to_date(cleaned, "MM/dd/yyyy"),
        f.try_to_date(cleaned, "dd/MM/yyyy"),
        f.try_to_date(cleaned, "yyyy/MM/dd")
    )


# 1. STORES

stores_expectations = {
    "valid_opened_date": "opened_date IS NOT NULL AND opened_date <= current_date()",
    "valid_is_active": "is_active IS NOT NULL",
    "valid_city": "city != 'Brak'",
    "valid_region": "region != 'Brak'",
    "valid_store_type": "store_type != 'Brak'"
}

@dp.table(
    name="silver_layer.silver_stores",
    schema="""
        store_id INT,
        store_name STRING, 
        city STRING, 
        region STRING, 
        opened_date DATE, 
        is_active BOOLEAN, 
        store_type STRING
    """
)
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_all(stores_expectations)
def silver_stores():
    df = spark.read.table("bronze_layer.bronze_stores")
    df = df.withColumns({
        "store_id": f.col("store_id").cast("int"),
        "store_name": f.coalesce(
            clean_empty_strings("store_name"),
            f.concat_ws(" ", f.lit("Sklep"), f.initcap(f.col("store_type")), f.col("city"))
        ),
        "opened_date": parse_date("opened_date"),
        "is_active": parse_boolean("is_active"),
        "city": f.coalesce(clean_empty_strings("city"), f.lit("Brak")),
        "region": f.coalesce(clean_empty_strings("region"), f.lit("Brak")),
        "store_type": f.coalesce(clean_empty_strings("store_type"), f.lit("Brak"))
    }).select(
        "store_id", "store_name", "city", "region", "opened_date", "is_active", "store_type"
    )

    return df


# 2. PRODUCTS

products_expectations = {
    "valid_product_name": "product_name != 'Brak'",
    "valid_category": "category != 'Brak'",
    "valid_brand": "brand != 'Brak'",
    "valid_unit_price": "unit_price IS NOT NULL AND unit_price > 0",
    "valid_cost_price": "cost_price IS NOT NULL AND cost_price > 0",
    "valid_weight_kg": "weight_kg IS NOT NULL AND weight_kg > 0",
    "valid_is_active": "is_active IS NOT NULL",
    "valid_created_date": "created_date IS NOT NULL AND created_date <= current_date()"
}

@dp.table(
    name="silver_layer.silver_products",
    schema=""" 
        product_id INT, 
        product_name STRING, 
        category STRING, 
        brand STRING, 
        unit_price DECIMAL(10, 2), 
        cost_price DECIMAL(10, 2), 
        stock_quantity INT, 
        is_active BOOLEAN, 
        created_date DATE, 
        weight_kg DOUBLE 
    """
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_all(products_expectations)
def silver_products():
    df = spark.read.table("bronze_layer.bronze_products")

    unit_price_num = f.col("unit_price").cast("decimal(10, 2)")
    cost_price_num = f.col("cost_price").cast("decimal(10, 2)")
    stock_qty_num = f.col("stock_quantity").cast("int")
    weight_kg_num = f.col("weight_kg").cast("double")

    category_clean = f.regexp_replace(clean_empty_strings("category"), r"\s*&\s*", " & ")

    df = df.withColumns({
        "product_id": f.col("product_id").cast("int"),
        "product_name": f.initcap(
            f.coalesce(clean_empty_strings("product_name"), f.lit("Brak"))
        ),
        "category": f.initcap(
            f.coalesce(category_clean, f.lit("Brak"))
        ),
        "brand": f.initcap(
            f.coalesce(clean_empty_strings("brand"), f.lit("Brak"))
        ),
        "unit_price": f.when(
            (unit_price_num == 0) | (f.abs(unit_price_num) >= 999999),
            f.lit(None)
        ).otherwise(f.abs(unit_price_num)),
        "cost_price": f.when(
            (cost_price_num == 0) | (f.abs(cost_price_num) >= 500000),
            f.lit(None)
        ).otherwise(f.abs(cost_price_num)),
        "stock_quantity": f.when(
            stock_qty_num.isNull() | (stock_qty_num < 0) | (stock_qty_num > 999999),
            f.lit(0)
        ).otherwise(stock_qty_num),
        "is_active": parse_boolean("is_active"),
        "created_date": parse_date("created_date"),
        "weight_kg": f.when(
            (weight_kg_num <= 0) | (weight_kg_num >= 999999),
            f.lit(None)
        ).otherwise(weight_kg_num)
    }).select(
        "product_id", "product_name", "category", "brand", "unit_price",
        "cost_price", "stock_quantity", "is_active", "created_date", "weight_kg"
    )

    return df


# 3. CUSTOMERS

customers_expectations = {
    "valid_first_name": "first_name != 'Brak'",
    "valid_last_name": "last_name != 'Brak'",
    "valid_email": "email IS NOT NULL",
    "valid_phone": "phone IS NOT NULL",
    "has_any_contact": "email IS NOT NULL OR phone IS NOT NULL",
    "valid_country": "country IS NOT NULL",
    "valid_gender": "gender IS NOT NULL",
    "valid_birth_year": "birth_year IS NOT NULL",
    "valid_registration_date": "registration_date IS NOT NULL AND registration_date <= current_date()",
    "valid_segment": "segment IS NOT NULL"
}

@dp.table(
    name="silver_layer.silver_customers",
    schema="""
        customer_id INT,
        first_name STRING, 
        last_name STRING, 
        email STRING, 
        phone STRING, 
        city STRING, 
        country STRING, 
        gender STRING, 
        birth_year INT,
        registration_date DATE, 
        segment STRING 
    """
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_all(customers_expectations)
def silver_customers():
    df = spark.read.table("bronze_layer.bronze_customers")

    phone_clean = f.regexp_replace(clean_empty_strings("phone"), r"[\s()\-]", "")
    phone_plus = f.regexp_replace(phone_clean, r"^00", "+")

    phone_with_prefix = f.when(
        phone_plus.rlike(r"^\d{9}$"), f.concat(f.lit("+48"), phone_plus)
    ).otherwise(phone_plus)

    final_phone = f.when(
        phone_with_prefix.rlike(r"^\+\d{7,14}$"), phone_with_prefix
    ).otherwise(f.lit(None))

    birth_year_num = f.col("birth_year").cast("int")

    df = df.withColumns({
        "customer_id": f.col("customer_id").cast("int"),
        "first_name": f.initcap(
            f.coalesce(clean_empty_strings("first_name"), f.lit("Brak"))
        ),
        "last_name": f.initcap(
            f.coalesce(clean_empty_strings("last_name"), f.lit("Brak"))
        ),
        "email": f.when(
            clean_empty_strings("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
            clean_empty_strings("email")
        ).otherwise(f.lit(None)),
        "phone": final_phone,
        "city": f.initcap(
            f.coalesce(clean_empty_strings("city"), f.lit("Brak"))
        ),
        "country": (
            f.when(f.lower(f.trim(f.col("country"))).isin("pl", "polska", "poland"), f.lit("PL"))
             .when(f.lower(f.trim(f.col("country"))).isin("de", "niemcy", "germany"), f.lit("DE"))
             .otherwise(f.lit(None))
        ),
        "gender": (
            f.when(f.lower(f.trim(f.col("gender"))).isin("m", "male"), f.lit("M"))
             .when(f.lower(f.trim(f.col("gender"))).isin("f", "female"), f.lit("F"))
             .otherwise(f.lit(None))
        ),
        "birth_year": f.when(
            (birth_year_num < 1920) | (birth_year_num > 2015), f.lit(None)
        ).otherwise(birth_year_num),
        "registration_date": parse_date("registration_date"),
        "segment": f.when(
            f.upper(f.trim(f.col("segment"))) == "VIP", f.lit("VIP")
        ).otherwise(f.initcap(clean_empty_strings("segment")))
    }).select(
        "customer_id", "first_name", "last_name", "email", "phone",
        "city", "country", "gender", "birth_year", "registration_date", "segment"
    )

    return df


# 4. ORDERS

orders_expectations = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_order_date": "order_date IS NOT NULL AND order_date <= current_date()",
    "valid_delivery_date": "delivery_date IS NOT NULL AND delivery_date >= order_date",
    "valid_status": "status != 'Brak'",
    "valid_channel": "channel != 'Brak'",
    "valid_payment_method": "payment_method != 'Brak'",
    "valid_total_amount": "total_amount IS NOT NULL AND total_amount > 0",
    "valid_discount_amount": "discount_amount >= 0",
    "valid_shipping_cost": "shipping_cost >= 0",
    "valid_store_id": "store_id IS NOT NULL"
}

@dp.table(
    name="silver_layer.silver_orders",
    schema="""
        order_id INT,
        customer_id INT,
        order_date DATE,
        delivery_date DATE,
        status STRING,
        channel STRING,
        payment_method STRING,
        total_amount DECIMAL(10, 2),
        discount_amount DECIMAL(10, 2),
        shipping_cost DECIMAL(10, 2),
        store_id INT
    """
)
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_all(orders_expectations)
def silver_orders():
    df = spark.readStream.table("bronze_layer.bronze_orders")

    order_date_parsed = parse_date("order_date")
    delivery_date_parsed = parse_date("delivery_date")

    total_amount_num = f.col("total_amount").cast("decimal(10, 2)")
    discount_amount_num = f.col("discount_amount").cast("decimal(10, 2)")
    shipping_cost_num = f.col("shipping_cost").cast("decimal(10, 2)")
    store_id_num = f.col("store_id").cast("int")

    channel_clean = clean_empty_strings("channel")
    payment_clean = f.regexp_replace(clean_empty_strings("payment_method"), "_", " ")

    df = df.withColumns({
        "order_id": f.col("order_id").cast("int"),
        "customer_id": f.col("customer_id").cast("int"),
        "order_date": order_date_parsed,
        "delivery_date": f.when(
            delivery_date_parsed < order_date_parsed,
            f.lit(None)
        ).otherwise(delivery_date_parsed),
        "status": f.initcap(
            f.coalesce(clean_empty_strings("status"), f.lit("Brak"))
        ),
        "channel": f.initcap(
            f.coalesce(channel_clean, f.lit("Brak"))
        ),
        "payment_method": f.when(
            f.upper(payment_clean) == "BLIK", f.lit("BLIK")
        ).otherwise(
            f.initcap(f.coalesce(payment_clean, f.lit("Brak")))
        ),
        "total_amount": f.when(
            (total_amount_num == 0) | (f.abs(total_amount_num) >= 999999),
            f.lit(None)
        ).otherwise(f.abs(total_amount_num)),
        "discount_amount": f.when(
            discount_amount_num.isNull() | (discount_amount_num < 0),
            f.lit(0).cast("decimal(10, 2)")
        ).otherwise(discount_amount_num),
        "shipping_cost": f.when(
            shipping_cost_num.isNull() | (shipping_cost_num < 0),
            f.lit(0).cast("decimal(10, 2)")
        ).otherwise(shipping_cost_num),
        "store_id": f.when(
            (store_id_num.isNull() | (store_id_num == 999)) & f.lower(channel_clean).isin("web", "mobile"),
            f.lit(105)
        ).when(
            store_id_num == 999,
            f.lit(None)
        ).otherwise(store_id_num)
    }).select(
        "order_id", "customer_id", "order_date", "delivery_date", "status",
        "channel", "payment_method", "total_amount", "discount_amount", "shipping_cost", "store_id"
    )

    return df


# 5. ORDER ITEMS

order_items_expectations = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_unit_price_at_sale": "unit_price_at_sale IS NOT NULL AND unit_price_at_sale > 0",
    "valid_discount_pct": "discount_pct BETWEEN 0 AND 0.50",
    "valid_returned": "returned IS NOT NULL"
}

@dp.table(
    name="silver_layer.silver_order_items",
    schema="""
        item_id INT,
        order_id INT,
        product_id INT,
        quantity INT,
        unit_price_at_sale DECIMAL(10, 2),
        discount_pct DECIMAL(5, 2),
        returned BOOLEAN
    """
)
@dp.expect_or_drop("valid_item_id", "item_id IS NOT NULL")
@dp.expect_all(order_items_expectations)
def silver_order_items():
    df = spark.readStream.table("bronze_layer.bronze_order_items")

    quantity_num = f.col("quantity").cast("double").cast("int")
    unit_price_num = f.col("unit_price_at_sale").cast("decimal(10, 2)")
    discount_pct_num = f.col("discount_pct").cast("decimal(5, 2)")

    df = df.withColumns({
        "item_id": f.col("item_id").cast("int"),
        "order_id": f.col("order_id").cast("int"),
        "product_id": f.col("product_id").cast("int"),
        "quantity": f.when(
            quantity_num == 0,
            f.lit(None).cast("int")
        ).otherwise(f.abs(quantity_num)),
        "unit_price_at_sale": f.when(
            (unit_price_num == 0) | (f.abs(unit_price_num) >= 999999),
            f.lit(None).cast("decimal(10, 2)")
        ).otherwise(f.abs(unit_price_num)),
        "discount_pct": f.when(
            discount_pct_num.isNull() | (discount_pct_num < 0),
            f.lit(0.00).cast("decimal(5, 2)")
        ).otherwise(discount_pct_num),
        "returned": parse_boolean("returned")
    }).select(
        "item_id", "order_id", "product_id", "quantity",
        "unit_price_at_sale", "discount_pct", "returned"
    )

    return df


# 6. RETURNS

returns_expectations = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_return_date": "return_date IS NOT NULL AND return_date <= current_date()",
    "valid_reason": "reason != 'Brak'",
    "valid_refund_amount": "refund_amount IS NOT NULL AND refund_amount > 0",
    "valid_return_status": "return_status != 'Brak'"
}

@dp.table(
    name="silver_layer.silver_returns",
    schema="""
        return_id INT,
        order_id INT,
        return_date DATE,
        reason STRING,
        refund_amount DECIMAL(10, 2),
        return_status STRING
    """
)
@dp.expect_or_drop("valid_return_id", "return_id IS NOT NULL")
@dp.expect_all(returns_expectations)
def silver_returns():
    df = spark.readStream.table("bronze_layer.bronze_returns")

    refund_amount_num = f.col("refund_amount").cast("decimal(10, 2)")

    df = df.withColumns({
        "return_id": f.col("return_id").cast("int"),
        "order_id": f.col("order_id").cast("int"),
        "return_date": parse_date("return_date"),
        "reason": f.initcap(
            f.coalesce(clean_empty_strings("reason"), f.lit("Brak"))
        ),
        "refund_amount": f.when(
            (refund_amount_num == 0) | (f.abs(refund_amount_num) >= 999999),
            f.lit(None).cast("decimal(10, 2)")
        ).otherwise(f.abs(refund_amount_num)),
        "return_status": f.initcap(
            f.coalesce(clean_empty_strings("return_status"), f.lit("Brak"))
        )
    }).select(
        "return_id", "order_id", "return_date", "reason", "refund_amount", "return_status"
    )

    return df
