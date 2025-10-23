from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date
from pyspark.sql.types import DoubleType, IntegerType

# ===============================
#  Initialize SparkSession
# ===============================
spark = (
    SparkSession.builder
    .appName("Clean_Silver_Hive_Iceberg")
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
#  Read Bronze Table (Iceberg)
# ===============================
df_bronze = spark.table("hive_catalog.bronze.online_retail_raw")

print(f"Original rows in Bronze: {df_bronze.count()}")
df_bronze.printSchema()

# ===============================
#  Data Cleaning
# ===============================

# 1️ Remove rows with missing key information
df_silver = df_bronze.dropna(
    subset=["customer_id", "order_date", "product_id", "product_name", "quantity", "price"]
)

# 2️ Remove duplicates
df_silver = df_silver.dropDuplicates()

# 3️ Clean text columns
df_silver = (
    df_silver
    .withColumn("product_name", trim(upper(col("product_name"))))
    .withColumn("category_name", trim(upper(col("category_name"))))
    .withColumn("city", trim(upper(col("city"))))
    .withColumn("payment_method", trim(upper(col("payment_method"))))
)

# 4️ Cast numeric columns to proper types
df_silver = (
    df_silver
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("price", col("price").cast(DoubleType()))
    .withColumn("review_score", col("review_score").cast(IntegerType()))
    .withColumn("age", col("age").cast(IntegerType()))
)

# 5️ Convert order_date to DateType
df_silver = df_silver.withColumn("order_date", to_date(col("order_date")))

# 6️ Calculate total revenue for each line item
df_silver = df_silver.withColumn("total_price", col("quantity") * col("price"))

# 7️ Filter out invalid records (quantity <= 0 or price <= 0)
df_silver = df_silver.filter((col("quantity") > 0) & (col("price") > 0))

print(f"Cleaned rows in Silver: {df_silver.count()}")

# ===============================
#  Write to Iceberg Silver Table
# ===============================
spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_catalog.silver")
spark.sql("DROP TABLE IF EXISTS hive_catalog.silver.ecommerce_clean")

(
    df_silver.writeTo("hive_catalog.silver.ecommerce_clean")
    .using("iceberg")
    .create()
)

print(" Silver table created: hive_catalog.silver.ecommerce_clean")

# ===============================
#  Verify
# ===============================
spark.sql("SHOW TABLES IN hive_catalog.silver").show()
spark.sql("SELECT * FROM hive_catalog.silver.ecommerce_clean LIMIT 10").show()

spark.stop()
