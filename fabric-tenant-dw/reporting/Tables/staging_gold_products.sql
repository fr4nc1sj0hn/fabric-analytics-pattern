CREATE TABLE [reporting].[staging_gold_products] (
    [product_id]          INT            NULL,
    [category_id]         INT            NULL,
    [cost_price]          FLOAT (53)     NULL,
    [list_price]          FLOAT (53)     NULL,
    [product_name]        VARCHAR (8000) NULL,
    [brand]               VARCHAR (8000) NULL,
    [tenant_id]           VARCHAR (8000) NULL,
    [ingestion_timestamp] DATETIME2 (6)  NULL,
    [ingestion_date]      DATE           NULL
);


GO

