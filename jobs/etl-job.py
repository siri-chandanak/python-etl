from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bronze-to-silver-pipeline").getOrCreate()

# =========================
# 1. SETUP (CREATE BRONZE TEST DATA IF NEEDED)
# =========================
spark.sql("USE bronze")

spark.sql("CREATE DATABASE IF NOT EXISTS default")

# Create test bronze table
spark.sql("""
CREATE TABLE IF NOT EXISTS default.users_bronze (
    id INT,
    name STRING
) USING iceberg
""")

spark.sql("INSERT INTO default.users_bronze VALUES (1,'a'),(2,'b')")

print("=== Bronze Data ===")
spark.sql("SELECT * FROM default.users_bronze").show()


# =========================
# 2. TRANSFORM
# =========================
bronze_df = spark.sql("SELECT * FROM default.users_bronze")

transformed_df = bronze_df.withColumn("name_upper", bronze_df["name"])

print("=== Transformed Data ===")
transformed_df.show()


# =========================
# 3. WRITE TO SILVER
# =========================
spark.sql("USE silver")
spark.sql("CREATE DATABASE IF NOT EXISTS default")

transformed_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable("default.users_silver")

print("=== Silver Data ===")
spark.sql("SELECT * FROM default.users_silver").show()


# =========================
# 4. WRITE TO GOLD
# =========================
spark.sql("USE gold")

gold_df = transformed_df.groupBy("name_upper").count()

gold_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable("default.users_gold")

print("=== Gold Data ===")
spark.sql("SELECT * FROM default.users_gold").show()


# =========================
# 5. CLEANUP (DELETE TABLES)
# =========================

print("=== Cleaning up tables ===")

# Delete GOLD
spark.sql("USE gold")
spark.sql("DROP TABLE IF EXISTS default.users_gold")

# Delete SILVER
spark.sql("USE silver")
spark.sql("DROP TABLE IF EXISTS default.users_silver")

# Delete BRONZE (optional — be careful in real env)
spark.sql("USE bronze")
spark.sql("DROP TABLE IF EXISTS default.users_bronze")

print("=== Cleanup completed ===")

spark.stop()