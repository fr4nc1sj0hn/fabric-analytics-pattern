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

tenantId = "tenant7"
runId = "test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from datetime import datetime

table_name = "control.data_quality_rules"

# source table
df = spark.table(table_name)

# get distinct entities
entities = [
    row["entity_name"]
    for row in df.select("entity_name").distinct().collect()
]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

for entity_name in entities:

    rules = (
        spark.table("control.data_quality_rules")
        .filter(col("entity_name") == entity_name)
        .filter(col("is_active") == True)
        .collect()
    )

    entity_name = 'sales.' + entity_name
    print(f"Processing DQ checks for: {entity_name}")

    # filter source data for current entity
    df = spark.table(entity_name)
    df_entity = df.filter(col("tenant_id") == tenantId)
    # load rules for entity
    
    for rule in rules:

        rule_type = rule["rule_type"]
        column_name = rule["column_name"]
        severity = rule["severity"]
        rule_value = rule["rule_value"]

        failed_count = 0

        # ==========================================
        # NOT NULL
        # ==========================================
        if rule_type == "NOT_NULL":

            failed_count = (
                df_entity
                .filter(col(column_name).isNull())
                .count()
            )

        # ==========================================
        # UNIQUE
        # ==========================================
        elif rule_type == "UNIQUE":

            failed_count = (
                df_entity
                .groupBy(column_name)
                .count()
                .filter(col("count") > 1)
                .count()
            )

        # ==========================================
        # ALLOWED VALUES
        # ==========================================
        elif rule_type == "ALLOWED_VALUES":

            allowed = [
                x.strip()
                for x in rule_value.split(",")
            ]

            failed_count = (
                df_entity
                .filter(~col(column_name).isin(allowed))
                .count()
            )

        # ==========================================
        # RESULT
        # ==========================================
        status = (
            "PASSED"
            if failed_count == 0
            else "FAILED"
        )

        results.append((
            runId,
            tenantId,
            entity_name,
            rule_type,
            column_name,
            failed_count,
            status,
            severity,
            datetime.utcnow()
        ))

print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result_columns = [
    "runId",
    "tenantId",
    "entity_name",
    "rule_type",
    "column_name",
    "failed_count",
    "status",
    "severity",
    "event_time"
]

df_results = spark.createDataFrame(results, result_columns)

(
    df_results.write
    .format("delta")
    .mode("append")
    .saveAsTable("control.data_quality_results")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
