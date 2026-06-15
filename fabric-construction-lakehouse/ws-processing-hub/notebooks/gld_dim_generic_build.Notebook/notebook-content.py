# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

tenant_id = "tenant1"
dim_name = "Dim_Activity"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================
# Generic Gold Dimension Builder Notebook (Metadata-Driven)
# ==============================================
# Purpose:
#   Generate Gold dimension tables from Silver tables
#   using relational metadata (no JSON mappings)
#
# Design Principles:
#   - Fully metadata-driven
#   - Reusable across tenants and domains
#   - Extensible (supports transforms, filtering, SCD later)
#   - Observable and idempotent
# ==============================================

from pyspark.sql import functions as F
import time

# ==============================================
# 1. PARAMETERS
# ==============================================

# Expected parameters (from Fabric pipeline or controller notebook)
# dim_name: string (e.g. dim_customer)
# tenant_id: string (optional, for multi-tenant silver)

tenant_id = "tenant1"
dim_name = "Dim_Activity"

assert dim_name, "dim_name is required"

start_time = time.time()
print(f"[INFO] Starting dimension build: {dim_name}")

# ==============================================
# 2. LOAD DIMENSION CONFIG (HEADER METADATA)
# ==============================================

df_tenant_config = spark.table("control.tenant_config")
df_dim_config = spark.table("control.dim_config")

config = (
    df_dim_config
    .filter((F.col("dim_name") == dim_name) & (F.col("is_active") == True))
    .collect()
)
tenant_config = (
    df_tenant_config
    .filter((F.col("tenant_id") == tenant_id) & (F.col("is_active") == True))
    .collect()
)
assert len(config) == 1, f"Config missing or duplicate for {dim_name}"

assert len(tenant_config) == 1, f"Config missing or duplicate for {tenant_id}"

config = config[0]
tenant_config = tenant_config[0]

gold_workspace = tenant_config["gold_workspace"]

source_table = config["source_table"]
target_table = config["target_table"]
#primary_key = config["primary_key"]
#filter_condition = config["filter_condition"]
filter_condition = None
scd_type = config["scd_type"]

print(f"[INFO] Source: {source_table} | Target: {target_table}")

# ==============================================
# 3. LOAD COLUMN MAPPING (RELATIONAL METADATA)
# ==============================================

df_mapping = (
    spark.table("control.dim_col_mapping")
    .filter((F.col("dim_name") == dim_name) & (F.col("is_active") == True))
)

mapping_rows = df_mapping.collect()

assert len(mapping_rows) > 0, f"No column mappings found for {dim_name}"

# ==============================================
# 4. EXTRACT SILVER DATA
# ==============================================

df_source = spark.table(source_table)

# Optional tenant filtering
if tenant_id:
    df_source = df_source.filter(F.col("tenant_id") == tenant_id)

# Optional filter condition from metadata
if filter_condition:
    df_source = df_source.filter(filter_condition)

# ==============================================
# 5. TRANSFORM (METADATA-DRIVEN COLUMN MAPPING)
# ==============================================

select_exprs = []

for row in mapping_rows:
    source_col = row["source_column"]
    target_col = row["target_column"]
    transform_expr = row["transform_expr"]

    if transform_expr:
        expr = F.expr(transform_expr).alias(target_col)
    else:
        expr = F.col(source_col).alias(target_col)

    select_exprs.append(expr)

# Apply projection

df_dim = df_source.select(*select_exprs)


# ==============================================
# 6. DATA QUALITY (BASIC)
# ==============================================

# Remove duplicates based on primary key

#df_dim = df_dim.dropDuplicates([primary_key])

# Add audit columns

df_dim = (
    df_dim
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

from pyspark.sql.functions import current_timestamp, lit

if tenant_id:
    df_dim = df_dim.withColumn("tenant_id", F.lit(tenant_id))

# ==============================================
# 7. LOAD (IDEMPOTENT WRITE)
# ==============================================

#build the target
target_table = "reporting_" + tenant_id + "." + target_table
print(f"[INFO] target_table: {target_table}")
if tenant_id:
    (
        df_dim.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"tenant_id = '{tenant_id}'")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )
else:
    (
        df_dim.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

# ==============================================
# 8. OBSERVABILITY
# ==============================================

row_count = df_dim.count()
elapsed = time.time() - start_time

print(f"[INFO] Rows written: {row_count}")
print(f"[INFO] Duration: {elapsed:.2f} seconds")

# Optional audit hook
# spark.createDataFrame([
#     (dim_name, tenant_id, row_count, elapsed, time.strftime('%Y-%m-%d %H:%M:%S'))
# ], ["dim_name", "tenant_id", "row_count", "duration_sec", "timestamp"]).write.mode("append").saveAsTable("control.audit_log")

print(f"[SUCCESS] Dimension build completed: {dim_name}")

# ==============================================
# END
# ==============================================


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================
# Generic Gold Dimension Builder Notebook (Metadata-Driven)
# ==============================================
# Purpose:
#   Generate Gold dimension tables from Silver tables
#   using relational metadata (no JSON mappings)
#
# Design Principles:
#   - Fully metadata-driven
#   - Reusable across tenants and domains
#   - Extensible (supports transforms, filtering, SCD later)
#   - Observable and idempotent
# ==============================================

from pyspark.sql import functions as F
import time

# ==============================================
# 1. PARAMETERS
# ==============================================

# Expected parameters (from Fabric pipeline or controller notebook)
# dim_name: string (e.g. dim_customer)
# tenant_id: string (optional, for multi-tenant silver)


assert dim_name, "dim_name is required"

start_time = time.time()
print(f"[INFO] Starting dimension build: {dim_name}")

# ==============================================
# 2. LOAD DIMENSION CONFIG (HEADER METADATA)
# ==============================================

df_tenant_config = spark.table("control.tenant_config")
df_dim_config = spark.table("control.dim_config")

config = (
    df_dim_config
    .filter((F.col("dim_name") == dim_name) & (F.col("is_active") == True))
    .collect()
)
tenant_config = (
    df_tenant_config
    .filter((F.col("tenant_id") == tenant_id) & (F.col("is_active") == True))
    .collect()
)
assert len(config) == 1, f"Config missing or duplicate for {dim_name}"

assert len(tenant_config) == 1, f"Config missing or duplicate for {tenant_id}"

config = config[0]
tenant_config = tenant_config[0]

gold_workspace = tenant_config["gold_workspace"]

source_table = config["source_table"]
target_table = config["target_table"]
#primary_key = config["primary_key"]
#filter_condition = config["filter_condition"]
filter_condition = None
scd_type = config["scd_type"]

print(f"[INFO] Source: {source_table} | Target: {target_table}")

# ==============================================
# 3. LOAD COLUMN MAPPING (RELATIONAL METADATA)
# ==============================================

df_mapping = (
    spark.table("control.dim_col_mapping")
    .filter((F.col("dim_name") == dim_name) & (F.col("is_active") == True))
)

mapping_rows = df_mapping.collect()

assert len(mapping_rows) > 0, f"No column mappings found for {dim_name}"

# ==============================================
# 4. EXTRACT SILVER DATA
# ==============================================

df_source = spark.table(source_table)

# Optional tenant filtering
if tenant_id:
    df_source = df_source.filter(F.col("tenant_id") == tenant_id)

# Optional filter condition from metadata
if filter_condition:
    df_source = df_source.filter(filter_condition)

# ==============================================
# 5. TRANSFORM (METADATA-DRIVEN COLUMN MAPPING)
# ==============================================

select_exprs = []

for row in mapping_rows:
    source_col = row["source_column"]
    target_col = row["target_column"]
    transform_expr = row["transform_expr"]

    if transform_expr:
        expr = F.expr(transform_expr).alias(target_col)
    else:
        expr = F.col(source_col).alias(target_col)

    select_exprs.append(expr)

# Apply projection

df_dim = df_source.select(*select_exprs)


# ==============================================
# 6. DATA QUALITY (BASIC)
# ==============================================

# Remove duplicates based on primary key

#df_dim = df_dim.dropDuplicates([primary_key])

# Add audit columns

df_dim = (
    df_dim
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

from pyspark.sql.functions import current_timestamp, lit

if tenant_id:
    df_dim = df_dim.withColumn("tenant_id", F.lit(tenant_id))

# ==============================================
# 7. LOAD (IDEMPOTENT WRITE)
# ==============================================

#build the target
target_table = "reporting_" + tenant_id + "." + target_table
print(f"[INFO] target_table: {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit



df_dim = spark.table("reporting_tenant1.gold_dim_activity_risk_d")

df_dim = (
    df_dim
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)


df_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("reporting_tenant1.gold_dim_activity_risk_d")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

def generate_hash_diff(df, cols, hash_col_name="hash_diff"):
    """
    Generate SHA256 hash from input columns.
    
    - Columns are sorted alphabetically (deterministic)
    - NULL-safe (converted to 'NULL')
    - Cast to string for consistency
    """
    
    sorted_cols = sorted(cols)

    return df.withColumn(
        hash_col_name,
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                    for c in sorted_cols
                ]
            ),
            256
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC ALTER TABLE reporting_tenant1.gold_dim_activity_risk_d
# MAGIC ADD COLUMNS (hash_diff STRING)


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_target_full = spark.table("reporting_tenant1.gold_dim_activity_risk_d")

if df_target_full.filter(F.col("hash_diff").isNull()).count() > 0:
    print("[INFO] Backfilling hash_diff for existing records...")

    df_target_full = generate_hash_diff(df_target_full, tracked_cols)

    (
        df_target_full.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pk_cols = [r["target_column"] for r in mapping_rows if r["is_business_key"]]

merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])

print(merge_condition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit

df_dim = (
    df_dim
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_target = spark.table(target_table)
count = df_target.count()
print(count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, target_table)

(
    delta_table.alias("t")
    .merge(df_dim.alias("s"), merge_condition)

    # 1. Expire old records if changed
    .whenMatchedUpdate(
        condition=f"t.is_current = true AND ({change_condition})",
        set={
            "effective_to": "current_timestamp()",
            "is_current": "false"
        }
    )

    # 2. Insert new records
    .whenNotMatchedInsertAll()

    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_target = spark.table(target_table)
count = df_target.count()
print(count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_target.groupBy(["effective_from"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_dim.groupBy(["effective_from"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_target[df_target["is_current"] == True].count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


dim_table = spark.table(target_table)
count = dim_table.count()
print(count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dim_table.groupBy(["is_current"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dim_table.groupBy(["effective_to"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dim_table.groupBy(["effective_from"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ----------------------------------------------
# 3. LOAD TARGET TABLE
# ----------------------------------------------

df_target = spark.table(target_table)

# ----------------------------------------------
# 4. GENERATE HASH COLUMN (NULL-SAFE)
# ----------------------------------------------

df_hashed = df_target.withColumn(
    "hash_diff",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.concat(
                    F.lit(c + "="),   # include column name (extra safety)
                    F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                )
                for c in tracked_cols
            ]
        ),
        256
    )
)

# ----------------------------------------------
# 5. WRITE BACK (OVERWRITE WITH SCHEMA UPDATE)
# ----------------------------------------------

(
    df_hashed.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print("[SUCCESS] hash_diff column created/updated successfully")

# ==============================================
# END
# ==============================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_silver.reporting_tenant1.gold_dim_activity LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
import time
from pyspark.sql.functions import current_timestamp, lit

tenant_id = "tenant1"
dim_name = "Dim_Activity"

# ==============================================
# Generic Gold Dimension Builder Notebook (Metadata-Driven)
# ==============================================
# Purpose:
#   Generate Gold dimension tables from Silver tables
#   using relational metadata (no JSON mappings)
#
# Design Principles:
#   - Fully metadata-driven
#   - Reusable across tenants and domains
#   - Extensible (supports transforms, filtering, SCD later)
#   - Observable and idempotent
# ==============================================


# ==============================================
# 1. PARAMETERS
# ==============================================

# Expected parameters (from Fabric pipeline or controller notebook)
# dim_name: string (e.g. dim_customer)
# tenant_id: string (optional, for multi-tenant silver)


assert dim_name, "dim_name is required"

start_time = time.time()
print(f"[INFO] Starting dimension build: {dim_name}")

# ==============================================
# 2. LOAD DIMENSION CONFIG (HEADER METADATA)
# ==============================================

df_tenant_config = spark.table("control.tenant_config")
df_dim_config = spark.table("control.dim_config")

config = (
    df_dim_config
    .filter((F.col("dim_name") == dim_name) & (F.col("is_active") == True))
    .collect()
)
tenant_config = (
    df_tenant_config
    .filter((F.col("tenant_id") == tenant_id) & (F.col("is_active") == True))
    .collect()
)
assert len(config) == 1, f"Config missing or duplicate for {dim_name}"

assert len(tenant_config) == 1, f"Config missing or duplicate for {tenant_id}"

config = config[0]
tenant_config = tenant_config[0]

gold_workspace = tenant_config["gold_workspace"]

source_table = config["source_table"]
target_table = config["target_table"]
#primary_key = config["primary_key"]
#filter_condition = config["filter_condition"]
filter_condition = None
scd_type = config["scd_type"]

print(f"[INFO] Source: {source_table} | Target: {target_table}")

# ==============================================
# 3. LOAD COLUMN MAPPING (RELATIONAL METADATA)
# ==============================================

df_mapping = (
    spark.table("control.dim_col_mapping")
    .filter((F.col("dim_name") == dim_name) & (F.col("is_active") == True))
)

mapping_rows = df_mapping.collect()

assert len(mapping_rows) > 0, f"No column mappings found for {dim_name}"

# ==============================================
# 4. EXTRACT SILVER DATA
# ==============================================

df_source = spark.table(source_table)

# Optional tenant filtering
if tenant_id:
    df_source = df_source.filter(F.col("tenant_id") == tenant_id)

# Optional filter condition from metadata
if filter_condition:
    df_source = df_source.filter(filter_condition)

# ==============================================
# 5. TRANSFORM (METADATA-DRIVEN COLUMN MAPPING)
# ==============================================

select_exprs = []

for row in mapping_rows:
    source_col = row["source_column"]
    target_col = row["target_column"]
    transform_expr = row["transform_expr"]

    if transform_expr:
        expr = F.expr(transform_expr).alias(target_col)
    else:
        expr = F.col(source_col).alias(target_col)

    select_exprs.append(expr)

# Apply projection

df_dim = df_source.select(*select_exprs)


# ==============================================
# 6. DATA QUALITY (BASIC)
# ==============================================

# Remove duplicates based on primary key

#df_dim = df_dim.dropDuplicates([primary_key])

# Add audit columns

df_dim = (
    df_dim
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
)



if tenant_id:
    df_dim = df_dim.withColumn("tenant_id", F.lit(tenant_id))

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ==============================================
# 1. IDENTIFY KEYS AND TRACKED COLUMNS
# ==============================================

pk_cols = [r["target_column"] for r in mapping_rows if r["is_business_key"]]
tracked_cols = [r["target_column"] for r in mapping_rows if r["is_scd_type2"]]

assert len(pk_cols) > 0, "No business keys defined"
assert len(tracked_cols) > 0, "No SCD2 tracked columns defined"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================
# 2. PREPARE INCOMING DATA (HASH + DEDUP)
# ==============================================

# Add hash for change detection (NULL-safe)
df_incoming = df_dim.withColumn(
    "hash_diff",
    F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                for c in tracked_cols
            ]
        ),
        256
    )
)

# Deduplicate based on business key (important for correctness)
df_incoming = df_incoming.dropDuplicates(pk_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_incoming)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================
# 3. LOAD CURRENT ACTIVE TARGET DATA
# ==============================================
target_table = 'reporting_tenant1.' +  target_table
df_target = spark.table(target_table).filter(F.col("is_current") == True)

# ==============================================
# 4. JOIN INCOMING VS TARGET
# ==============================================

join_condition = [
    df_incoming[c] == df_target[c] for c in pk_cols
]

df_joined = (
    df_incoming.alias("s")
    .join(df_target.alias("t"), join_condition, "left")
    .select(
        *[F.col(f"s.{c}").alias(c) for c in df_incoming.columns],   # source columns
        *[F.col(f"t.{c}").alias(f"t_{c}") for c in df_target.columns]  # target columns (prefixed)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new = df_joined.filter(F.col("t_hash_diff").isNull())


df_changed = df_joined.filter(
    (F.col("t_hash_diff").isNotNull()) &
    (F.col("t_hash_diff") != F.col("hash_diff"))
)

df_unchanged = df_joined.filter(
    F.col("hash_diff") == F.col("t_hash_diff")
)

print(df_new.count())
print(df_changed.count())
print(df_unchanged.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================
# 6. EXPIRE OLD RECORDS (FOR CHANGED ROWS)
# ==============================================

delta_table = DeltaTable.forName(spark, target_table)

merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])

(
    delta_table.alias("t")
    .merge(
        df_changed.select(*pk_cols).alias("s"),
        merge_condition
    )
    .whenMatchedUpdate(
        condition="t.is_current = true",
        set={
            "effective_to": "current_timestamp()",
            "is_current": "false"
        }
    )
    .execute()
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, col

df_expired = spark.table(target_table).filter(
    (col("is_current") == False) &
    (col("effective_to").isNotNull())
)

display(df_expired)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_inserts = (
    df_new
    .union(df_changed)
    .select([c for c in df_new.columns if not c.startswith("t_")])
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)
print(df_inserts.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================
# 8. INSERT NEW VERSIONS
# ==============================================

df_inserts.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(target_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_target = spark.table(target_table)
print(df_target.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_target.groupBy(["effective_from"]).count())
display(df_target.groupBy(["effective_to"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# ==============================================
# 9. OPTIONAL: OBSERVABILITY
# ==============================================

print(f"[INFO] New records: {df_new.count()}")
print(f"[INFO] Changed records: {df_changed.count()}")
print(f"[INFO] Unchanged records: {df_unchanged.count()}")

print("[SUCCESS] SCD Type 2 load completed")

# ==============================================
# END
# ==============================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
