CREATE       PROCEDURE [reporting].[usp_apply_scd_from_mapping]
    @SourceSchema sysname = 'reporting',
    @SourceTable  sysname = 'staging_gold_products',
    @TargetSchema sysname = 'reporting',
    @TargetTable  sysname = 'gold_dim_products',
    @MappingSchema sysname = N'reporting',
    @MappingTable  sysname = N'column_mapping',
    @Debug bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @AsOfDate datetime2(3) = GETUTCDATE();
    DECLARE @SourceFull nvarchar(400);
    DECLARE @TargetFull nvarchar(400);
    DECLARE @JoinPred   nvarchar(max);
    DECLARE @PKJoin   nvarchar(max);
    DECLARE @Type1Set   nvarchar(max);
    DECLARE @Type1Diff  nvarchar(max);
    DECLARE @Type2Diff  nvarchar(max);
    DECLARE @InsertCols nvarchar(max);
    DECLARE @InsertVals nvarchar(max);
    DECLARE @PKCol      sysname;
    DECLARE @sql        nvarchar(max);
    SET @SourceFull = QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@SourceTable);
    SET @TargetFull = QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@TargetTable);
    SELECT TOP (1)
        @PKCol = QUOTENAME(target_column)
    FROM reporting.column_mapping
    WHERE target_table = @TargetTable
      AND is_primary_key = 1;
    IF @PKCol IS NULL
        THROW 50001, 'No primary key mapping found.', 1;
	
    SELECT
    @PKJoin =
        STRING_AGG(
            CASE
                WHEN is_primary_key = 1 THEN
                    N's.' + QUOTENAME(source_column) +
                    N' = t.' + QUOTENAME(target_column)
            END,
            N' AND '
        ),
    @Type1Set =
        STRING_AGG(
            CASE
                WHEN is_scd_type1 = 1 THEN
                    N't.' + QUOTENAME(target_column) +
                    N' = s.' + QUOTENAME(source_column)
            END,
            N', '
        ),
    @Type1Diff =
        STRING_AGG(
            CASE
                WHEN is_scd_type1 = 1
                 AND track_changes = 1 THEN
                    N'ISNULL(CONVERT(nvarchar(max),s.' +
                    QUOTENAME(source_column) +
                    N'),''<NULL>'') <> ' +
                    N'ISNULL(CONVERT(nvarchar(max),t.' +
                    QUOTENAME(target_column) +
                    N'),''<NULL>'')'
            END,
            N' OR '
        ),
    @Type2Diff =
        STRING_AGG(
            CASE
                WHEN is_scd_type2 = 1
                 AND track_changes = 1 THEN
                    N'ISNULL(CONVERT(nvarchar(max),s.' +
                    QUOTENAME(source_column) +
                    N'),''<NULL>'') <> ' +
                    N'ISNULL(CONVERT(nvarchar(max),t.' +
                    QUOTENAME(target_column) +
                    N'),''<NULL>'')'
            END,
            N' OR '
        ),
    @InsertCols =
        STRING_AGG(QUOTENAME(target_column), N', '),
    @InsertVals =
        STRING_AGG(
            N's.' + QUOTENAME(source_column),
            N', '
        )
    FROM reporting.column_mapping
    WHERE target_table = @TargetTable;
	SET @InsertCols = @InsertCols + N',[tenant_id],[ingestion_date],[valid_from],[valid_to],[is_active]'
	SET @InsertVals = @InsertVals + N',s.[tenant_id],s.[ingestion_date]'
    SET @sql = N'
BEGIN TRY
    BEGIN TRAN;
    IF OBJECT_ID(''tempdb..#chg'') IS NOT NULL
        DROP TABLE #chg;
	-- Type 2 SCD
	WITH dim AS 
	(
		SELECT * FROM ' + @TargetFull + N' WHERE is_active = 1
	)
    SELECT s.*, IIF(t.' + @PKCol + N' IS NULL,''New'',''Changed'') AS Change_Type
    INTO #chg
    FROM ' + @SourceFull + N' s
    LEFT JOIN dim t
        ON ' + @PKJoin + N'
    WHERE t.' + @PKCol + N' IS NULL
       OR (' + ISNULL(@Type2Diff, N'1=0') + N');
	-- Expire old rows
	UPDATE t
       SET t.is_active = 0,
           t.valid_to = GETUTCDATE()
    FROM ' + @TargetFull + N' t
    JOIN #chg c
      ON ' + REPLACE(@PKJoin, N's.', N'c.') + N'
    WHERE t.is_active = 1;
	-- New Rows for SCD2
	INSERT INTO ' + @TargetFull + N'
    (
        ' + @InsertCols + N'
    )
    SELECT
        ' + @InsertVals + N',
        CAST(GETUTCDATE() AS DATE),
        CONVERT(date, ''9999-12-31''),
        1
    FROM #chg s
    WHERE s.Change_Type = ''Changed''
	-- New
	INSERT INTO ' + @TargetFull + N'
    (
        ' + @InsertCols + N'
    )
    SELECT
        ' + @InsertVals + N',
        CAST(GETUTCDATE() AS DATE),
        CONVERT(date, ''9999-12-31''),
        1
    FROM #chg s
    WHERE s.Change_Type = ''New''
	
	-- Type 1
	' + CASE
            WHEN NULLIF(@Type1Set, N'') IS NOT NULL THEN
N'
    UPDATE t
       SET ' + @Type1Set + N'
    FROM ' + @TargetFull + N' t
    JOIN ' + @SourceFull + N' s
      ON ' + @PKJoin + N'
    WHERE t.is_active = 1
      AND (' + ISNULL(@Type1Diff, N'1=0') + N');
'
            ELSE N''
        END + N'
    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN;
    THROW;
END CATCH;
';
    IF @Debug = 1
    BEGIN
        SELECT @sql AS generated_sql;
        RETURN;
    END;
    BEGIN TRY
        EXEC sp_executesql @sql
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN;
        THROW;
    END CATCH
END;

GO
