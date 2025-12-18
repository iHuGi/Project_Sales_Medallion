import dlt
from pyspark.sql.functions import (
    col, to_date, when, current_timestamp, sum as spark_sum,
    count, year, round, avg, date_format, lag, first, month, min, max,
    coalesce, lit, expr, regexp_replace,
    trim, initcap, upper
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

# --- CONFIGURATION ---
# Pointing to your NEW Git Volume paths
CUSTOMER_DATA_PATH = "/Volumes/project_sales_medallion_git/project_sales_git/files/customer_dataset.json"
RETAIL_DATA_PATH = "/Volumes/project_sales_medallion_git/project_sales_git/files/retail_dataset.csv"

# HELPER: Date Parser
def parse_date_col(column_name):
    """Normalizes mixed date formats (yyyy-MM-dd, dd-MM-yyyy, etc.) into standard DateType"""
    clean_col = regexp_replace(col(column_name), "/", "-")
    return coalesce(
        to_date(clean_col, "yyyy-MM-dd"),
        to_date(clean_col, "dd-MM-yyyy"),
        to_date(clean_col, "MM-dd-yyyy")
    )

# =========================================
# BRONZE LAYER (RAW DATA)
# =========================================

@dlt.table(
    name="customers_bronze_git",
    comment="Raw customers JSON - Git Version",
    table_properties={"pipelines.autoCaptureSchemaChanges": "true"}
)
def customers_bronze_git():
    return (spark.read
            .option("multiline", "true")
            .option("inferSchema", "true")
            .json(CUSTOMER_DATA_PATH)
            .withColumn("load_timestamp", current_timestamp()))

@dlt.table(
    name="retail_bronze_git",
    comment="Raw retail CSV - Git Version",
    table_properties={"pipelines.autoCaptureSchemaChanges": "true"}
)
def retail_bronze_git():
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(RETAIL_DATA_PATH)
            .withColumn("load_timestamp", current_timestamp()))


# =========================================
# SILVER LAYER (CLEANED & VALIDATED)
# =========================================

@dlt.table(
    name="customers_silver_git",
    comment="Cleaned customers - Git Version"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers_silver_git():
    df = dlt.read("customers_bronze_git")
    
    return df.withColumn("signup_date_parsed", parse_date_col("signup_date")) \
             .withColumn("gender_std", 
                when(col("gender").ilike("f") | col("gender").ilike("female"), lit("F"))
                .when(col("gender").ilike("m") | col("gender").ilike("male"), lit("M"))
                .otherwise(lit(None))) \
             .select(
                trim(col("customer_id")).alias("customer_id"),
                col("age").cast(IntegerType()).alias("age"),
                initcap(trim(coalesce(col("city"), lit("Unknown")))).alias("city"),
                col("gender_std").alias("gender"),
                upper(trim(col("loyalty_tier"))).alias("loyalty_tier"),
                col("signup_date_parsed").alias("signup_date"),
                "load_timestamp"
            )

@dlt.table(
    name="retail_silver_git",
    comment="Cleaned retail transactions - Git Version"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
# FIX: Check 'quantity_int' because 'quantity' was dropped in the select()
@dlt.expect_or_drop("valid_quantity", "quantity_int > 0") 
def retail_silver_git():
    df = dlt.read("retail_bronze_git")

    return df.withColumn("quantity_int", col("quantity").cast(IntegerType())) \
             .withColumn("price_double", col("price").cast(DoubleType())) \
             .withColumn("order_date_parsed", parse_date_col("order_date")) \
             .select(
                trim(col("order_id")).alias("order_id"),
                trim(col("customer_id")).alias("customer_id"),
                "order_date_parsed",
                initcap(trim(col("product_name"))).alias("product"),
                col("category"),
                "quantity_int",
                "price_double",
                trim(col("order_status")).alias("status"),
                col("returned"),
                "load_timestamp"
            )


# =========================================
# GOLD LAYER (ANALYTICS & KPIs)
# =========================================

# 1. Customer Spending Summary
@dlt.table(
    name="customer_spending_summary_git",
    comment="Total lifetime spent, order count, and first/last order dates per customer."
)
def customer_spending_summary_git():
    df_retail = dlt.read("retail_silver_git")
    df_customers = dlt.read("customers_silver_git")

    customer_agg = df_retail.groupBy("customer_id").agg(
        round(spark_sum("price_double"), 2).alias("total_spent_lifetime"),
        count("order_id").alias("num_orders"),
        min("order_date_parsed").alias("first_order_date"),
        max("order_date_parsed").alias("last_order_date")
    )
    
    return customer_agg.join(df_customers, on="customer_id", how="left") \
        .select(
            "customer_id", "total_spent_lifetime", "num_orders", 
            "first_order_date", "last_order_date", "city", "loyalty_tier", "age", "gender"
        )

# 2. Sales by City
@dlt.table(
    name="sales_by_city_git",
    comment="Total revenue, orders, and AOV aggregated by city."
)
def sales_by_city_git():
    df_retail = dlt.read("retail_silver_git")
    df_customers = dlt.read("customers_silver_git")
    
    # We join first to get the City from the customer table
    df = df_retail.join(df_customers, on="customer_id", how="left")
    
    return df.groupBy("city").agg(
        round(spark_sum("price_double"), 2).alias("city_total_revenue"),
        count("order_id").alias("city_total_orders"),
        round(avg("price_double"), 2).alias("city_avg_order_value")
    ).orderBy(col("city_total_revenue").desc())

# 3. Product Performance
@dlt.table(
    name="product_category_performance_git",
    comment="Revenue, units sold, and returns by product/category."
)
def product_category_performance_git():
    df = dlt.read("retail_silver_git")
    
    return df.groupBy("category", "product").agg(
        round(spark_sum("price_double"), 2).alias("gross_revenue"),
        spark_sum("quantity_int").alias("units_sold"),
        count("order_id").alias("total_transactions"),
        count(when(col("returned") == "Yes", 1)).alias("returned_count"),
        (
            round(spark_sum("price_double"), 2) -
            round(spark_sum(when(col("returned") == "Yes", col("price_double")).otherwise(0)), 2)
        ).alias("net_revenue")
    ).orderBy(col("net_revenue").desc())

# 4. Monthly Trends (MoM Growth)
@dlt.table(
    name="monthly_sales_trends_git",
    comment="Monthly time series data with MoM growth calculations."
)
def monthly_sales_trends_git():
    df = dlt.read("retail_silver_git")
    
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
    
    return (monthly_agg
        .withColumn("prev_month_revenue", 
            coalesce(lag("monthly_revenue").over(window_spec), lit(0)))
        .withColumn("mom_growth_pct", 
            when(col("prev_month_revenue") == 0, lit(None))
            .otherwise(
                round(((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100, 2)
            )
        )
        .orderBy(col("month_key").asc())
    )