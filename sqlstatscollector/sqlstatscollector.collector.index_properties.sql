SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

----------------------------------------------------------------
-- Collector: [index_properties]
-- Definition: Structure v1
----------------------------------------------------------------

RAISERROR(N'Collector: [index_properties]', 10, 1) WITH NOWAIT
GO

----------------------------------------------------------------
-- Table [data].[index_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[index_properties] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'index_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xEDC4A886A29B0C5020FB053BB8178D9B8D1F8F39CE74AED00BD56EAE5F26E82B

DECLARE @TableExists int
DECLARE @TableHasChanged int
DECLARE @FullName nvarchar(255)
DECLARE @NewName nvarchar(128)

DECLARE @cmd nvarchar(2048)
DECLARE @msg nvarchar(2048)

SELECT @FullName = [FullName]
     , @TableExists = [TableExists]
     , @TableHasChanged = [TableHasChanged]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

IF @TableExists = 1 AND @TableHasChanged = 1
BEGIN
	SELECT @cmd = N'DROP TABLE ' + @FullName
	RAISERROR(@cmd, 10, 1) WITH NOWAIT
	EXEC (@cmd)
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	SELECT @msg = N'Creating ' + @FullName
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	CREATE TABLE [data].[index_properties](
		[collector_rowguid] [uniqueidentifier] NOT NULL CONSTRAINT [df_index_properties_rowguid] DEFAULT NEWID() ROWGUIDCOL,
		[database_id] [int] NOT NULL,
		[object_id] [int] NOT NULL,
		[table_name] [nvarchar](128) NOT NULL,
		[index_id] [int] NOT NULL,
		[index_name] [nvarchar](128) NOT NULL,
		[index_type] [nvarchar](60) NOT NULL,
		[fill_factor] [tinyint] NOT NULL,
		[index_key_columns] [nvarchar](3000) NULL,
		[index_included_columns] [nvarchar](3000) NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_index_properties] PRIMARY KEY CLUSTERED 
			(
				[database_id] ASC, 
				[object_id] ASC, 
				[index_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


----------------------------------------------------------------
-- Trigger [data].[index_properties_change]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Trigger [data].[index_properties_change] ******/', 10, 1) WITH NOWAIT
GO
CREATE OR ALTER TRIGGER [data].[index_properties_change]
ON [data].[index_properties]
AFTER UPDATE
AS
BEGIN

    INSERT INTO [data].[collector_changes] ([rowtimeutc], [collector_rowguid], [collectorname] , [propertyname], [old_value], [new_value])
    SELECT i.[LastUpdatedUTC], i.[collector_rowguid], N'index_properties', changedata.propertyname, changedata.old_value, changedata.new_value
    FROM inserted i INNER JOIN deleted d ON i.[database_id] = d.[database_id] AND i.[object_id] = d.[object_id] AND i.[index_id] = d.[index_id] 
    CROSS APPLY ( VALUES 
    -- Insert a list of columns for change tracking here.

                  (N'[table_name]'             , CAST(d.[table_name] AS nvarchar(256))                          , CAST(i.[table_name] AS nvarchar(256)))
                 ,(N'[index_name]'             , CAST(d.[index_name] AS nvarchar(256))                          , CAST(i.[index_name] AS nvarchar(256)))
                 ,(N'[index_type]'             , CAST(d.[index_type] AS nvarchar(256))                          , CAST(i.[index_type] AS nvarchar(256)))
                 ,(N'[fill_factor]'            , CAST(d.[fill_factor] AS nvarchar(256))                         , CAST(i.[fill_factor] AS nvarchar(256)))
                 ,(N'[index_key_columns]'      , ISNULL(CAST(d.[index_key_columns] AS nvarchar(MAX)), '')      , ISNULL(CAST(i.[index_key_columns] AS nvarchar(MAX)), ''))
                 ,(N'[index_included_columns]' , ISNULL(CAST(d.[index_included_columns] AS nvarchar(MAX)), '') , ISNULL(CAST(i.[index_included_columns] AS nvarchar(MAX)), ''))

    --End of column list             
    ) changedata (propertyname ,old_value ,new_value)
    WHERE changedata.old_value <> changedata.new_value

END
GO


----------------------------------------------------------------
-- StoredProcedure [collect].[index_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  StoredProcedure [collect].[index_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[index_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[index_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[index_properties]
   -----------------------------------------
   Collects information about the indexes in all databases.
   This information is kept as one row per index

Date		Name				Description
----------	-------------		-----------------------------------------------
2026-11-16	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [collect].[index_properties]
AS
BEGIN
PRINT('[collect].[index_properties] - Collection of metadata of indexes.')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'index_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @index_properties TABLE (
		[database_id] [int] NOT NULL,
		[object_id] [int] NOT NULL,
		[table_name] [nvarchar](128) NOT NULL,
		[index_id] [int] NOT NULL,
		[index_name] [nvarchar](128) NOT NULL,
		[index_type] [nvarchar](60) NOT NULL,
		[fill_factor] [tinyint] NOT NULL,
		[index_key_columns] [nvarchar](3000) NULL,
		[index_included_columns] [nvarchar](3000) NULL
		)

	DECLARE	@dbname sysname
	DECLARE @SQL varchar(2048);
	DECLARE @databases TABLE ([name] nvarchar(255) )

	DECLARE @v varchar(20)

	BEGIN TRY

		SELECT @v = [internal].[GetSQLServerVersion]()

		IF (@v IN ('2005', '2008', '2008R2'))
		BEGIN
			INSERT INTO @databases([name])
			SELECT d.[name]
				FROM sys.databases d
				WHERE d.[name] NOT IN ('tempdb')
				AND d.[state_desc] = 'ONLINE'
		END
		ELSE
		BEGIN
			INSERT INTO @databases([name])
			SELECT d.[name]
				FROM sys.databases d LEFT OUTER JOIN sys.dm_hadr_availability_replica_states rs
				ON rs.replica_id = d.replica_id
				WHERE d.[name] NOT IN ('tempdb')
				AND d.[state_desc] = 'ONLINE'
				AND ISNULL(rs.role_desc, N'PRIMARY') = N'PRIMARY'
		END
		;

		/* Loop all online databases except tempdb */
		DECLARE databaselist CURSOR
			LOCAL STATIC FORWARD_ONLY READ_ONLY
			FOR SELECT [name] FROM @databases
		
		OPEN databaselist;
		FETCH NEXT FROM databaselist INTO @dbname;
		WHILE @@FETCH_STATUS = 0
		BEGIN

			SET @SQL = 'USE [' + @dbname +'];' + char(10) + char(13)
			SET @SQL = @SQL + 
			'SELECT [database_id] = DB_ID()
			, [object_id] = ix.[object_id]
			, [table_name] = N''['' + OBJECT_SCHEMA_NAME(ix.[object_id]) + N''].['' + OBJECT_NAME(ix.[object_id]) + N'']''
			, [index_id] = ix.[index_id]
			, [index_name] = ISNULL(ix.[name], N''HEAP'')
			, [index_type] = ix.[type_desc]
			, [fill_factor] = ix.[fill_factor]
			, [index_key_columns] = CAST(SUBSTRING((
		           SELECT N'', ['' + _ac.[name] + N'']''
		           FROM sys.[tables] AS _t 
		           INNER JOIN sys.[indexes] _i ON _t.[object_id] = _i.[object_id] 
		           INNER JOIN sys.[index_columns] _ic ON _i.[object_id] = _ic.[object_id] AND _i.[index_id] = _ic.[index_id] 
		           INNER JOIN sys.[all_columns] _ac ON _t.[object_id] = _ac.[object_id] AND _ic.[column_id] = _ac.[column_id] 
		           WHERE ix.[object_id] = _i.[object_id] AND ix.[index_id] = _i.[index_id] AND _ic.[is_included_column] = 0 
		           ORDER BY _ic.[key_ordinal]  
		           FOR  XML PATH('''') )
		           , 3, 3000) as nvarchar(3000))
			, [index_included_columns] = CAST(SUBSTRING((
				   SELECT N'', ['' + _ac.[name] + N'']''
				   FROM sys.[tables] AS _t 
				   INNER JOIN sys.[indexes] _i ON _t.[object_id] = _i.[object_id] 
				   INNER JOIN sys.[index_columns] _ic ON _i.[object_id] = _ic.[object_id] AND _i.[index_id] = _ic.[index_id] 
				   INNER JOIN sys.[all_columns] _ac ON _t.[object_id] = _ac.[object_id] AND _ic.[column_id] = _ac.[column_id] 
				   WHERE ix.[object_id] = _i.[object_id] AND ix.[index_id] = _i.[index_id] AND _ic.[is_included_column] = 1 
				   ORDER BY _ic.[key_ordinal]  
				   FOR XML PATH('''') )
				   , 3, 3000) as nvarchar(3000))
			FROM sys.indexes ix WITH(NOLOCK) 
			WHERE OBJECTPROPERTY(ix.[object_id], ''IsUserTable'') = 1' 
			+ char(10) + char(13)

			BEGIN TRY

				/* Get system information about indexes */
				INSERT INTO @index_properties
				EXEC (@SQL);

			END TRY
			BEGIN CATCH 
				SET @SQL = 'Failure running commands against ' + @dbname + error_message()
				PRINT @SQL
			END CATCH

			SET @SQL = ''


			FETCH NEXT FROM databaselist INTO @dbname;
		END

		MERGE [data].[index_properties] dest USING
		(SELECT [database_id], [object_id], [table_name], [index_id], [index_name]
		 , [index_type], [fill_factor], [index_key_columns], [index_included_columns]
		 FROM @index_properties i) src 
		   ON src.[database_id] = dest.[database_id] 
		   AND src.[object_id] = dest.[object_id]
		   AND src.[index_id] = dest.[index_id]
		WHEN NOT MATCHED THEN
				INSERT ([database_id], [object_id], [table_name], [index_id], [index_name]
					  , [index_type], [fill_factor], [index_key_columns], [index_included_columns] ,[LastUpdatedUTC])
				VALUES (src.[database_id], src.[object_id], src.[table_name], src.[index_id], src.[index_name]
				      , src.[index_type], src.[fill_factor], src.[index_key_columns], src.[index_included_columns], SYSUTCDATETIME())
		WHEN MATCHED THEN
				UPDATE SET 
				[table_name] = src.[table_name]
			   ,[index_name] = src.[index_name]
			   ,[index_type] = src.[index_type]
			   ,[fill_factor] = src.[fill_factor]
			   ,[index_key_columns] = src.[index_key_columns]
			   ,[index_included_columns] = src.[index_included_columns]
			   ,[LastUpdatedUTC] = SYSUTCDATETIME()
		WHEN NOT MATCHED BY SOURCE THEN	  
		        UPDATE SET 
			    [index_type] = N'DELETED'
			   ,[index_key_columns] = NULL
			   ,[index_included_columns] = NULL
			   ,[LastUpdatedUTC] = SYSUTCDATETIME();

	END TRY
	BEGIN CATCH
		DECLARE @msg nvarchar(4000)
		SELECT @error = ERROR_NUMBER(), @msg = ERROR_MESSAGE()
		PRINT (@msg)
	END CATCH

	SELECT @current_end = SYSUTCDATETIME()
	UPDATE [internal].[executionlog]
	SET [EndTimeUTC] = @current_end
	, [Duration_ms] =  ((CAST(DATEDIFF(S, @current_start, @current_end) AS bigint) * 1000000) + (DATEPART(MCS, @current_end)-DATEPART(MCS, @current_start))) / 1000.0
	, [errornumber] = @@ERROR
	WHERE [Id] = @current_logitem

END
GO

----------------------------------------------------------------
-- StoredProcedure [transfer].[index_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  StoredProcedure [transfer].[index_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[index_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[index_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[index_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2026-11-16	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[index_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	DECLARE @index_properties TABLE (
		[serverid] [uniqueidentifier] NOT NULL,
		[collector_rowguid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[object_id] [int] NOT NULL,
		[table_name] [nvarchar](128) NOT NULL,
		[index_id] [int] NOT NULL,
		[index_name] [nvarchar](128) NOT NULL,
		[index_type] [nvarchar](60) NOT NULL,
		[fill_factor] [tinyint] NOT NULL,
		[index_key_columns] [nvarchar](3000) NULL,
		[index_included_columns] [nvarchar](3000) NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL)

	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
	OUTPUT @serverid serverid 
	     , inserted.[collector_rowguid]
	     , inserted.[database_id]
		 , inserted.[object_id]
		 , inserted.[table_name]
		 , inserted.[index_id]
		 , inserted.[index_name]
		 , inserted.[index_type]
		 , inserted.[fill_factor]
		 , inserted.[index_key_columns]
		 , inserted.[index_included_columns]
		 , inserted.[LastUpdatedUTC]
		 , inserted.[LastHandledUTC]
    INTO @index_properties
	FROM [data].[index_properties] s
	WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]

	SELECT dp.[serverid]
	     , dp.[collector_rowguid]
	     , dp.[database_id]
		 , dp.[object_id]
		 , dp.[table_name]
		 , dp.[index_id]
		 , dp.[index_name]
		 , dp.[index_type]
		 , dp.[fill_factor]
		 , dp.[index_key_columns]
		 , dp.[index_included_columns]
		 , dp.[LastUpdatedUTC]
		 , dp.[LastHandledUTC]
    FROM @index_properties dp

END
GO

----------------------------------------------------------------
-- Finalizing [index_properties]
----------------------------------------------------------------
RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'index_properties', '0 6 * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
