from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gravitino-show-catalogs").getOrCreate()

print("=== initial SHOW CATALOGS ===")
spark.sql("SHOW CATALOGS").show(truncate=False)

print("=== USE bronze ===")
spark.sql("USE bronze")

print("=== SHOW CURRENT NAMESPACE ===")
spark.sql("SELECT current_catalog(), current_database()").show(truncate=False)

print("=== SHOW CATALOGS after USE bronze ===")
spark.sql("SHOW CATALOGS").show(truncate=False)

spark.sql("USE bronze")
spark.sql("USE gold")
spark.sql("USE silver")
spark.sql("SHOW CATALOGS").show()

spark.stop()

