# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000000",
# META       "default_lakehouse_name": "core",
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

# Welcome to your new notebook
# Type here in the cell editor to add code!
TenantId = "tenant4"
TenantName = "Tenant 4"
CapacityId = "00000000-0000-0000-0000-000000000000"
ConnectionId = "00000000-0000-0000-0000-000000000000"
WorkspaceId = "00000000-0000-0000-0000-000000000000"
WarehouseId = "00000000-0000-0000-0000-000000000000"
ConnectionString = "sample-sql-endpoint.datawarehouse.fabric.microsoft.com"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import current_date
from delta.tables import DeltaTable

WorkspaceName = "ws_" +  TenantId
WarehouseName = "dw_" + TenantId

data = [
    Row(
        TenantId = TenantId,
        TenantName = TenantName,
        CapacityId = CapacityId,
        ConnectionId = ConnectionId,
        WorkspaceId = WorkspaceId,
        WorkspaceName = WorkspaceName,
        WarehouseId = WarehouseId,
        ConnectionString = ConnectionString,
        WarehouseName = WarehouseName,
        CreatedBy = 'system',
        IsActive = True,
        DeletionFlag = False 
    )
]
df = spark.createDataFrame(data) \
        .withColumn("DateCreated", current_date()) \
        .withColumn("LastUpdate", current_date())


target_table = DeltaTable.forName(spark, "core.control.tenant_info")

(
    target_table.alias("t")
        .merge(
            df.alias("s"),
            "t.TenantId = s.TenantId"
        )
        .whenMatchedUpdate(set={
            "TenantName": "s.TenantName",
            "CapacityId": "s.CapacityId",
            "ConnectionId": "s.ConnectionId",
            "WorkspaceId": "s.WorkspaceId",
            "WorkspaceName": "s.WorkspaceName",
            "WarehouseId": "s.WarehouseId",
            "ConnectionString": "s.ConnectionString",
            "WarehouseName": "s.WarehouseName",
            "CreatedBy": "s.CreatedBy",
            "IsActive": "s.IsActive",
            "DeletionFlag": "s.DeletionFlag",
            "LastUpdate": "s.LastUpdate"
        })
        .whenNotMatchedInsert(values={
            "TenantId": "s.TenantId",
            "TenantName": "s.TenantName",
            "CapacityId": "s.CapacityId",
            "ConnectionId": "s.ConnectionId",
            "WorkspaceId": "s.WorkspaceId",
            "WorkspaceName": "s.WorkspaceName",
            "WarehouseId": "s.WarehouseId",
            "ConnectionString": "s.ConnectionString",
            "WarehouseName": "s.WarehouseName",
            "DateCreated": "s.DateCreated",
            "CreatedBy": "s.CreatedBy",
            "IsActive": "s.IsActive",
            "LastUpdate": "s.LastUpdate",
            "DeletionFlag": "s.DeletionFlag"
        })
        .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
