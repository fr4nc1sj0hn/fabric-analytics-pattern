CREATE TABLE [reporting].[column_mapping] (
    [column_map_id]        VARCHAR (8000) NULL,
    [table_map_id]         VARCHAR (8000) NULL,
    [source_table]         VARCHAR (8000) NULL,
    [source_column]        VARCHAR (8000) NULL,
    [target_column]        VARCHAR (8000) NULL,
    [target_table]         VARCHAR (8000) NULL,
    [transformation_logic] VARCHAR (8000) NULL,
    [is_scd_type1]         BIT            NULL,
    [is_scd_type2]         BIT            NULL,
    [track_changes]        BIT            NULL,
    [is_primary_key]       BIT            NULL,
    [is_nullable]          BIT            NULL,
    [is_active]            BIT            NULL
);


GO

