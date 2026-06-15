CREATE TABLE [reporting].[table_mapping] (
    [table_map_id]       INT            NULL,
    [pipeline_id]        VARCHAR (8000) NULL,
    [source_table]       VARCHAR (8000) NULL,
    [silver_table]       VARCHAR (8000) NULL,
    [gold_table]         VARCHAR (8000) NULL,
    [primary_table]      BIT            NULL,
    [load_strategy]      VARCHAR (8000) NULL,
    [watermark_column]   VARCHAR (8000) NULL,
    [tenant_id]          INT            NULL,
    [created_at]         DATETIME2 (6)  NULL,
    [is_active]          BIT            NULL,
    [staging_gold_table] VARCHAR (8000) NULL
);


GO

