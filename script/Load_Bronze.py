from pyspark.sql import SparkSession

# ===============================
#  Initialize SparkSession
# ===============================
spark = (
    SparkSession.builder
    .appName("Load_Bronze_Hive_Iceberg")
    # Enable Hive Metastore
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    # Iceberg catalog using Hive Metastore
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://tuankiet170-master:9000/data_lake/warehouse")
    # Enable Iceberg SQL Extensions
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Optional tuning
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .config("spark.sql.catalog.hive_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate()
)

# ===============================
#  Load raw CSV data (Bronze)
# ===============================
csv_path = "hdfs://tuankiet170-master:9000/data_lake/bronze/online_retail/online_retail.csv"

df_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .csv(csv_path)
)

print(f" Raw data loaded successfully: {df_bronze.count()} rows")
df_bronze.printSchema()

# ===============================
#  Create namespace if missing
# ===============================
spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_catalog.bronze")

# ===============================
#  Drop old Iceberg table if exists
# ===============================
spark.sql("DROP TABLE IF EXISTS hive_catalog.bronze.online_retail_raw")
print(" Old table dropped (if existed)")

# ===============================
#  Write data to Iceberg table
# ===============================
(
    df_bronze.writeTo("hive_catalog.bronze.online_retail_raw")
    .using("iceberg")
    .create()
)

print(" Iceberg table created fresh: hive_catalog.bronze.online_retail_raw")

# ===============================
#  Verify the result
# ===============================
spark.sql("SHOW TABLES IN hive_catalog.bronze").show()
spark.sql("SELECT * FROM hive_catalog.bronze.online_retail_raw LIMIT 5").show()

# ===============================
#  Stop Spark session
# ===============================
spark.stop()
