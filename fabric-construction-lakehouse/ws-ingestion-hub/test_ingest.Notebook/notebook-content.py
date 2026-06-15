# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, current_date

# --------------------------------------------
# 1. READ FROM BRONZE (SAME WORKSPACE)
# --------------------------------------------
source_path = "/lakehouse/default/Files/bronze/tenant1/20260416/W_RESOURCE_D.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(source_path)

# --------------------------------------------
# 2. ADD METADATA
# --------------------------------------------
df = df.withColumn("ingestion_timestamp", current_timestamp()) \
       .withColumn("ingestion_date", current_date())

# --------------------------------------------
# 3. CLEAN
# --------------------------------------------
df = df.dropDuplicates()

# --------------------------------------------
# 4. WRITE TO SILVER (DELTA TABLE)
# --------------------------------------------
df.write \
  .format("delta") \
  .mode("append") \
  .partitionBy("ingestion_date") \
  .saveAsTable("W_RESOURCE_D")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
