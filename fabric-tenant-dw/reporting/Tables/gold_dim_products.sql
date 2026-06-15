CREATE TABLE [reporting].[gold_dim_products] (
    [product_id]     INT           NOT NULL,
    [category_id]    INT           NOT NULL,
    [cost_price]     FLOAT (53)    NOT NULL,
    [list_price]     FLOAT (53)    NOT NULL,
    [product_name]   VARCHAR (100) NOT NULL,
    [brand]          VARCHAR (100) NOT NULL,
    [tenant_id]      VARCHAR (50)  NOT NULL,
    [ingestion_date] DATE          NULL,
    [valid_from]     DATE          NULL,
    [valid_to]       DATE          NULL,
    [is_active]      BIT           NULL
);


GO

