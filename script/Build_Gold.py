from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, year, month, weekofyear, dayofmonth, quarter, dayofweek, monotonically_increasing_id
)

# ===============================
#  Initialize SparkSession
# ===============================
spark = (
    SparkSession.builder
    .appName("Build_Gold_Layer_Iceberg")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://tuankiet170-master:9000/data_lake/warehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# ===============================
#  Load Silver table
# ===============================
df_silver = spark.table("hive_catalog.silver.ecommerce_clean")
df_silver.printSchema()

# ===============================
#  Create Namespace Gold
# ===============================
spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_catalog.gold")

# ===============================
#  DIM TABLES
# ===============================

# --- DimCustomer ---
dim_customer = (
    df_silver.select("customer_id", "gender", "age")
    .dropDuplicates(["customer_id"])
    .withColumnRenamed("customer_id", "CustomerID")
    .withColumn("CustomerKey", monotonically_increasing_id())
)
dim_customer.writeTo("hive_catalog.gold.dim_customer").using("iceberg").createOrReplace()

# --- DimProduct ---
dim_product = (
    df_silver.select("product_id", "product_name")
    .dropDuplicates(["product_id"])
    .withColumnRenamed("product_id", "ProductID")
    .withColumnRenamed("product_name", "ProductName")
    .withColumn("ProductKey", monotonically_increasing_id())
)
dim_product.writeTo("hive_catalog.gold.dim_product").using("iceberg").createOrReplace()

# --- DimCategory ---
dim_category = (
    df_silver.select("category_id", "category_name")
    .dropDuplicates(["category_id"])
    .withColumnRenamed("category_id", "CategoryID")
    .withColumnRenamed("category_name", "CategoryName")
    .withColumn("CategoryKey", monotonically_increasing_id())
)
dim_category.writeTo("hive_catalog.gold.dim_category").using("iceberg").createOrReplace()

# --- DimCity ---
dim_city = (
    df_silver.select("city")
    .dropDuplicates(["city"])
    .withColumnRenamed("city", "CityName")
    .withColumn("CityKey", monotonically_increasing_id())
)
dim_city.writeTo("hive_catalog.gold.dim_city").using("iceberg").createOrReplace()

# --- DimPayment ---
dim_payment = (
    df_silver.select("payment_method")
    .dropDuplicates(["payment_method"])
    .withColumnRenamed("payment_method", "PaymentMethod")
    .withColumn("PaymentKey", monotonically_increasing_id())
)
dim_payment.writeTo("hive_catalog.gold.dim_payment").using("iceberg").createOrReplace()

# --- DimDate ---
dim_date = (
    df_silver.select("order_date").dropDuplicates()
    .withColumnRenamed("order_date", "FullDate")
    .withColumn("Year", year(col("FullDate")))
    .withColumn("Quarter", quarter(col("FullDate")))
    .withColumn("Month", month(col("FullDate")))
    .withColumn("Week", weekofyear(col("FullDate")))
    .withColumn("Day", dayofmonth(col("FullDate")))
    .withColumn("DayOfWeek", dayofweek(col("FullDate")))
    .withColumn("DateKey", monotonically_increasing_id())
)
dim_date.writeTo("hive_catalog.gold.dim_date").using("iceberg").createOrReplace()

# ===============================
#  FACT TABLE
# ===============================

# Join dimension keys to build FactSales
fact_sales = (
    df_silver
    .join(dim_customer, df_silver.customer_id == dim_customer.CustomerID, "left")
    .join(dim_product, df_silver.product_id == dim_product.ProductID, "left")
    .join(dim_category, df_silver.category_id == dim_category.CategoryID, "left")
    .join(dim_city, upper(trim(df_silver.city)) == upper(trim(dim_city.CityName)), "left")
    .join(dim_payment, upper(trim(df_silver.payment_method)) == upper(trim(dim_payment.PaymentMethod)), "left")
    .join(dim_date, df_silver.order_date == dim_date.FullDate, "left")
    .select(
        monotonically_increasing_id().alias("SaleID"),
        col("CustomerKey"),
        col("ProductKey"),
        col("CategoryKey"),
        col("DateKey"),
        col("PaymentKey"),
        col("CityKey"),
        col("quantity").alias("Quantity"),
        col("price").alias("Price"),
        col("total_price").alias("TotalAmount"),
        col("review_score").alias("ReviewScore")
    )
)

fact_sales.writeTo("hive_catalog.gold.fact_sales").using("iceberg").createOrReplace()

print(" Successfully built Gold Layer (Fact & Dimensions).")

# ===============================
#  Verify Results
# ===============================
print("\n=== GOLD TABLES ===")
spark.sql("SHOW TABLES IN hive_catalog.gold").show(truncate=False)

print("\n=== FACT SAMPLE ===")
spark.sql("SELECT * FROM hive_catalog.gold.fact_sales LIMIT 10").show(truncate=False)

spark.stop()
