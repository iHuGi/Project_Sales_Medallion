import dlt
from pyspark.sql.functions import (
    col, to_date, when, current_timestamp, sum as spark_sum,
    count, year, round, avg, date_format, lag, first, month, min, max,
    coalesce, lit, expr, regexp_replace,
    trim, initcap, upper
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

# --- CONFIGURATION (Paths need to be correct for your environment) ---
CUSTOMER_DATA_PATH = "/Volumes/project_sales_medallion/project_sales/files/customer_dataset.json"
RETAIL_DATA_PATH = "/Volumes/project_sales_medallion/project_sales/files/retail_dataset.csv"

# =========================================
# BRONZE LAYER (RAW DATA) - WRITE FIXED
# =========================================
@dlt.table(
    name="sales_bronze.customers_bronze",
    comment="Raw customers JSON with load timestamp.",
    table_properties={"pipelines.autoCaptureSchemaChanges": "true"}
)
def customers_bronze():
    df = (spark.read
          .option("multiline", "true")
          .option("inferSchema", "true")
          .json(CUSTOMER_DATA_PATH)
         )
    return df.withColumn("load_timestamp", current_timestamp())

@dlt.table(
    name="sales_bronze.retail_bronze",
    comment="Raw retail CSV with load timestamp.",
    table_properties={"pipelines.autoCaptureSchemaChanges": "true"}
)
def retail_bronze():
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(RETAIL_DATA_PATH)
         )
    return df.withColumn("load_timestamp", current_timestamp())


# =========================================
# SILVER LAYER (CLEANED DATA) - READ & WRITE FIXED
# =========================================

# 📌 Customers Silver (Reads from sales_bronze.customers_bronze)
@dlt.table(
    name="sales_silver.customers_silver",
    comment="Cleaned and validated customers data."
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers_silver():
    # FIX: Read from the fully qualified Bronze table name
    df = dlt.read("sales_bronze.customers_bronze") 
    
    df = df.withColumn(
    "signup_date_clean",
    regexp_replace(col("signup_date"), "/", "-")
    ).withColumn(
    "signup_date_parsed",
    coalesce(
        to_date(col("signup_date_clean"), "yyyy-MM-dd"),
        to_date(col("signup_date_clean"), "dd-MM-yyyy"),
        to_date(col("signup_date_clean"), "MM-dd-yyyy")
        )
    )
    
    df = df.withColumn(
        "gender_standardized",
        when(col("gender").ilike("f") | col("gender").ilike("female"), lit("F"))
        .when(col("gender").ilike("m") | col("gender").ilike("male"), lit("M"))
        .otherwise(lit(None))
    )
    
    df = df.withColumn(
    "city_clean",
    initcap(trim(coalesce(col("city"), lit("Unknown"))))
    )

    df = df.withColumn(
    "loyalty_tier_clean",
    upper(trim(col("loyalty_tier")))
    )

    df = df.withColumn(
        "age_clean",
        when(col("age") < 0, lit(None)).otherwise(col("age"))
    )

    return df.select(
    trim(col("customer_id")).alias("customer_id"),
    col("age_clean").cast(IntegerType()).alias("age"),
    col("city_clean").alias("city"),
    col("gender_standardized").alias("gender"),
    col("loyalty_tier_clean").alias("loyalty_tier"),
    col("signup_date_parsed").alias("signup_date"),
    "load_timestamp"
    )

# 📌 Retail Silver (Reads from sales_bronze.retail_bronze)
@dlt.table(
    # FIX: Corrected table name to sales_silver.retail_silver for consistency
    name="sales_silver.retail_silver", 
    comment="Cleaned and validated retail transaction dataset."
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity", "quantity_int IS NOT NULL AND quantity_int > 0")
@dlt.expect_or_drop("valid_price", "price_double IS NOT NULL AND price_double > 0")
def retail_silver():
    # FIX: Read from the fully qualified Bronze table name
    df = dlt.read("sales_bronze.retail_bronze") 

    df_clean = df.select(
    trim(col("order_id")).alias("order_id"),
    trim(col("customer_id")).alias("customer_id"),
    trim(col("customer_name")).alias("customer_name"),
    col("order_date").alias("order_date_raw"),
    trim(col("product_id")).alias("product_id"),
    initcap(trim(coalesce(col("product_name"), lit("Unknown")))).alias("product"),
    trim(col("category")).alias("category"),
    trim(col("quantity")).alias("quantity"),
    trim(col("price")).alias("price"),
    trim(col("payment_type")).alias("payment_method"),
    trim(col("order_status")).alias("status"),
    trim(col("returned")).alias("returned"),
    col("load_timestamp")
    ).withColumn(
    "quantity_int",
    when(col("quantity").rlike("^[0-9]+$"), trim(col("quantity")).cast(IntegerType()))
    ).withColumn(
    "price_double",
    when(col("price").rlike("^[0-9]+(\\.[0-9]+)?$"), trim(col("price")).cast(DoubleType()))
    ).withColumn(
    "order_date_clean",
    regexp_replace(col("order_date_raw"), "/", "-")
    ).withColumn(
    "order_date_parsed",
    coalesce(
        to_date(col("order_date_clean"), "yyyy-MM-dd"),
        to_date(col("order_date_clean"), "dd-MM-yyyy"),
        to_date(col("order_date_clean"), "MM-dd-yyyy")
        )
    )

    return df_clean

# =========================================
# GOLD LAYER (ANALYTICS & KPI TABLES) - READ & WRITE FIXED
# =========================================

# 1. Total spent per customer 
@dlt.table(
    name="sales_gold.customer_spending_summary", # FIX: Simplified table name
    comment="Total lifetime spent, order count, and first/last order dates per customer."
)
def gold_customer_spending_summary():
    # FIX: Read from the fully qualified Silver table names
    df_retail = dlt.read("sales_silver.retail_silver") 
    df_customers = dlt.read("sales_silver.customers_silver")

    customer_agg = df_retail.groupBy("customer_id", "customer_name").agg(
        round(spark_sum("price_double"), 2).alias("total_spent_lifetime"),
        count("order_id").alias("num_orders"),
        min("order_date_parsed").alias("first_order_date"),
        max("order_date_parsed").alias("last_order_date")
    )
    
    return customer_agg.join(df_customers, on="customer_id", how="left") \
        .select(
            "customer_id", "customer_name", "total_spent_lifetime", "num_orders", 
            "first_order_date", "last_order_date", "city", "loyalty_tier", "age", "gender"
        )


# 2. Sales Performance by Geography (City)
@dlt.table(
    name="sales_gold.sales_by_city", # FIX: Simplified table name
    comment="Total revenue, orders, and AOV aggregated by city."
)
def gold_sales_by_city():
    # FIX: Read from the fully qualified Silver table names
    df_retail = dlt.read("sales_silver.retail_silver")
    df_customers = dlt.read("sales_silver.customers_silver")
    
    df = df_retail.join(df_customers, on="customer_id", how="left")
    
    return df.groupBy("city").agg(
        round(spark_sum("price_double"), 2).alias("city_total_revenue"),
        count("order_id").alias("city_total_orders"),
        round(avg("price_double"), 2).alias("city_avg_order_value")
    ).orderBy(col("city_total_revenue").desc())


# 3. Product/Category Performance Summary
@dlt.table(
    name="sales_gold.product_category_performance", # FIX: Simplified table name
    comment="Revenue, units sold, return count, and price range by product and category."
)
def gold_product_category_performance():
    # FIX: Read from the fully qualified Silver table name
    df = dlt.read("sales_silver.retail_silver") 
    return df.groupBy(
        "category", "product", "product_id"
    ).agg(
        round(spark_sum("price_double"), 2).alias("gross_revenue"),
        spark_sum("quantity_int").alias("units_sold"),
        count("order_id").alias("total_transactions"),
        
        round(min("price_double"), 2).alias("min_price"),
        round(max("price_double"), 2).alias("max_price"),

        count(when(col("returned") == "Yes", 1)).alias("returned_count"),
        (
            round(spark_sum("price_double"), 2) -
            round(spark_sum(when(col("returned") == "Yes", col("price_double")).otherwise(0)), 2)
        ).alias("net_revenue")
    ).orderBy(col("net_revenue").desc())


# 4. Monthly/Time Series Trends (for MoM Growth Analysis)
@dlt.table(
    name="sales_gold.monthly_sales_trends",
    comment="Monthly time series data with MoM growth calculations."
)
def gold_monthly_sales_trends():
    df = dlt.read("sales_silver.retail_silver")
    
    monthly_agg = df.groupBy(
        year("order_date_parsed").alias("year"),
        month("order_date_parsed").alias("month"),
        date_format("order_date_parsed", "yyyy-MM").alias("month_key")
    ).agg(
        round(spark_sum("price_double"), 2).alias("monthly_revenue"),
        count("order_id").alias("monthly_orders"),
        round(avg("price_double"), 2).alias("monthly_aov")
    )
    
    window_spec = Window.orderBy("year", "month")
    
    df_with_prev = monthly_agg.withColumn(
        "prev_month_revenue", 
        # FIX 1: Use coalesce with 0 as default for the first month's LAG.
        # This handles the initial NULL, making the subsequent WHEN/OTHERWISE cleaner.
        coalesce(lag("monthly_revenue").over(window_spec), lit(0))
    )
    
    return df_with_prev.withColumn(
        "mom_growth_pct", 
        # FIX 2: Use WHEN/OTHERWISE to prevent division by zero (or NULLs).
        when(
            col("prev_month_revenue") == 0,
            lit(None) # Set growth to NULL when there's no prior revenue baseline
        ).otherwise(
            round(
                ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100, 
                2
            )
        )
    ).orderBy(col("month_key").asc())