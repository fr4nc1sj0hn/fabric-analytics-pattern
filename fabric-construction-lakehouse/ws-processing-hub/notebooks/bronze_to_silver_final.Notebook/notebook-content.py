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
source_table = "bronze_orders"
ingestion_date_param = "20260506"
target_table = "silver_orders"
run_id = "test"
warehouse_id = "test"
Environment = "DEV"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import requests
from pyspark.sql.functions import col, to_date, lit, current_timestamp, current_date, col

# -------------------------------------------------
# Logging helper
# -------------------------------------------------
def get_log_function_url():
    row = (
        spark.table("control.config")
        .filter(
            (col("config_key") == "LOG_FUNCTION_URL") &
            (col("Environment") == Environment)
        )
        .select("config_value")
        .limit(1)
        .first()
    )

    if row is None or not row[0]:
        raise ValueError("LOG_FUNCTION_URL not found in control.config")

    return row[0]


def log_event(
    status,
    message,
    details,
    duration_seconds,
    event_type="BRONZE_TO_SILVER",
    component="ELT",
    error_message=None
):
    url = get_log_function_url()

    payload = {
        "run_id": run_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
        "component": component,
        "details": details,
        "status": status,
        "message": message if not error_message else f"{message}: {error_message}",
        "duration_seconds": duration_seconds
    }

    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
        print(f"Log sent successfully: {response.status_code}")
    except Exception as log_error:
        # Do not fail the main job just because logging failed
        print(f"Logging failed: {log_error}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# -------------------------------------------------
# Main load
# -------------------------------------------------
start_time = time.time()

try:
    # handle empty string from pipeline
    if ingestion_date_param == "":
        ingestion_date_param = None

    # -------------------------------------------------
    # Ingestion date logic
    # -------------------------------------------------
    if ingestion_date_param:
        ingestion_date_col = to_date(lit(ingestion_date_param), "yyyyMMdd")
        ingestion_date_str = (
            f"{ingestion_date_param[:4]}-"
            f"{ingestion_date_param[4:6]}-"
            f"{ingestion_date_param[6:8]}"
        )
    else:
        ingestion_date_col = current_date()
        ingestion_date_str = spark.sql("SELECT current_date()").collect()[0][0].strftime("%Y-%m-%d")

    # -------------------------------------------------
    # Build bronze path
    # -------------------------------------------------
    file_name = f"{source_table}.csv"
    if ingestion_date_param:
        source_path = f"Files/sales/tenant_id={tenant_id}/ingestion_date={ingestion_date_param}/{file_name}"
    else:
        raise Exception("ingestion_date is required for Bronze path")

    # -------------------------------------------------
    # Read bronze CSV
    # -------------------------------------------------
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
    )

    # -------------------------------------------------
    # Add metadata
    # -------------------------------------------------
    df = (
        df.withColumn("tenant_id", lit(tenant_id))
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("ingestion_date", ingestion_date_col)
    )

    # -------------------------------------------------
    # Cleaning
    # -------------------------------------------------
    df = df.dropDuplicates()

    # -------------------------------------------------
    # Target table
    # -------------------------------------------------
    silver_table = f"sales.{target_table}"

    # -------------------------------------------------
    # Safe partition overwrite
    # -------------------------------------------------
    replace_where = f"""
    tenant_id = '{tenant_id}'
    AND ingestion_date = DATE '{ingestion_date_str}'
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("tenant_id", "ingestion_date")
        .option("replaceWhere", replace_where)
        .option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )

    # -------------------------------------------------
    # Success log
    # -------------------------------------------------
    duration_seconds = round(time.time() - start_time, 2)
    log_details = f"warehouse_id: {warehouse_id}, source: {source_table}, target: {target_table}"
    log_event(
        status="SUCCESS",
        message="Loaded tables successfully",
        details=log_details,
        duration_seconds=duration_seconds
    )

except Exception as e:
    # -------------------------------------------------
    # Failure log
    # -------------------------------------------------
    duration_seconds = round(time.time() - start_time, 2)
    log_details = f"warehouse_id: {warehouse_id}, source: {source_table}, target: {target_table}"
    log_event(
        status="FAILED",
        message="Bronze to Silver load failed",
        details=log_details,
        duration_seconds=duration_seconds,
        error_message=str(e)
    )
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
