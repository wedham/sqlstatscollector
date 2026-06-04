SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


RAISERROR(N'Collector: [databasefile_properties]', 10, 1) WITH NOWAIT
GO
----------------------------------------------------------------
-- Table [data].[databasefile_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[databasefile_properties] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'databasefile_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xDE872F4E7FF352E269CACED6EBD201F7FE4713C5E78837761E1DF706100623D7

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
	CREATE TABLE [data].[databasefile_properties](
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[type_desc] [nvarchar](60) NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[physical_name] [nvarchar](260) NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[max_size_mb] [int] NULL,
		[growth_mb] [int] NULL,
		[growth_percent] [int] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_databasefile_properties] PRIMARY KEY CLUSTERED 
			(
				[database_id] ASC,
				[file_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table ' + [FullName] + ' was found with checksum ' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

----------------------------------------------------------------
-- Table [data].[databasefile_properties_changes]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[databasefile_properties_changes] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'databasefile_properties_changes'
DECLARE @TableDefinitionHash varbinary(32) = 0x3CE4565A8505B6C77F22481EC27E514DB09C032E6BCB45F49D036A1FE25A0E99

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
	CREATE TABLE [data].[databasefile_properties_changes](
		[rowtimeutc] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[propertyname] [nvarchar](128) NOT NULL,
		[old_value] [nvarchar](256) NOT NULL,
		[new_value] [nvarchar](256) NOT NULL,
	) ON [PRIMARY]
END

SELECT @msg = N'Table ' + [FullName] + ' was found with checksum ' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


----------------------------------------------------------------
-- Trigger [data].[databasefile_properties_change]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Trigger [data].[databasefile_properties_change] ******/', 10, 1) WITH NOWAIT
GO
CREATE OR ALTER TRIGGER [data].[databasefile_properties_change]
ON [data].[databasefile_properties]
AFTER UPDATE
AS
BEGIN

    INSERT INTO [data].[databasefile_properties_changes] ([rowtimeutc], [database_id], [file_id], [propertyname], [old_value], [new_value])
    SELECT i.[LastUpdatedUTC], i.[database_id], i.[file_id], changedata.propertyname, changedata.old_value, changedata.new_value
    FROM inserted i INNER JOIN deleted d ON i.[database_id] = d.[database_id] AND i.[file_id] = d.[file_id] 
    CROSS APPLY ( VALUES 
    -- Insert a list of columns for change tracking here.

                  (N'[type_desc]'      , CAST(d.[type_desc] AS nvarchar(256))      , CAST(i.[type_desc] AS nvarchar(256)))
                 ,(N'[name]'           , CAST(d.[name] AS nvarchar(256))           , CAST(i.[name] AS nvarchar(256)))
                 ,(N'[physical_name]'  , CAST(d.[physical_name] AS nvarchar(256))  , CAST(i.[physical_name] AS nvarchar(256)))
                 ,(N'[state_desc]'     , CAST(d.[state_desc] AS nvarchar(256))     , CAST(i.[state_desc] AS nvarchar(256)))
                 ,(N'[size_mb]'        , CAST(d.[size_mb] AS nvarchar(256))        , CAST(i.[size_mb] AS nvarchar(256)))
                 ,(N'[max_size_mb]'    , CAST(d.[max_size_mb] AS nvarchar(256))    , CAST(i.[max_size_mb] AS nvarchar(256)))
                 ,(N'[growth_mb]'      , CAST(d.[growth_mb] AS nvarchar(256))      , CAST(i.[growth_mb] AS nvarchar(256)))
                 ,(N'[growth_percent]' , CAST(d.[growth_percent] AS nvarchar(256)) , CAST(i.[growth_percent] AS nvarchar(256)))

    --End of column list             
    ) changedata (propertyname ,old_value ,new_value)
    WHERE changedata.old_value <> changedata.new_value


END
GO


----------------------------------------------------------------
-- StoredProcedure [collect].[databasefile_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  StoredProcedure [collect].[databasefile_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[databasefile_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[databasefile_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   ---------------------------------------
   [collect].[databasefile_properties]
   ---------------------------------------
   Collects all properties of individual database files.
   One row per file, updates only - no history.
   All filesizes are in whole Megabytes.
   Removed files are kept until cleanup is initiated

Date		Name				Description
--------	-------------		-----------------------------------------------
2022-01-23	Mikael Wedham		+Created v1
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-06-03	Mikael Wedham		History functionality added
*******************************************************************************/
ALTER PROCEDURE [collect].[databasefile_properties]
AS
BEGIN
PRINT('[collect].[databasefile_properties] - Get all properties of individual database files')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'databasefile_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	BEGIN TRY

		MERGE [data].[databasefile_properties] dest USING 
		(SELECT f.[database_id]
			, f.[file_id]
			, f.[type_desc] 
			, f.[name]
			, f.[physical_name] 
			, f.[state_desc] 
			, [size_mb] = CAST( (f.[size]/128.0) AS decimal(19,4) ) 
			, [max_size_mb] = CASE WHEN f.[max_size] = -1 THEN NULL
									ELSE CAST( f.[max_size]/128.0 AS int) END --Null means no limit
			, [growth_mb] = CASE WHEN f.[is_percent_growth] = 1 THEN NULL
								ELSE CAST(f.[growth]/128.0 as int) END --NULL if growth is percent
			, [growth_percent] = CASE WHEN f.[is_percent_growth] = 0 THEN NULL
								ELSE f.[growth] END --NULL if growth is in megabytes
		FROM sys.master_files f ) src
		ON src.[database_id] = dest.[database_id] AND src.[file_id] = dest.[file_id]  
		WHEN NOT MATCHED THEN
			INSERT ([database_id], [file_id], [type_desc], [name], [physical_name], [state_desc]
				, [size_mb], [max_size_mb] ,[growth_mb] ,[growth_percent] ,[LastUpdatedUTC]) 
			VALUES (src.[database_id], src.[file_id], src.[type_desc], src.[name], src.[physical_name], src.[state_desc]
				, src.[size_mb], src.[max_size_mb] ,src.[growth_mb] ,src.[growth_percent] ,SYSUTCDATETIME()) 
		WHEN MATCHED THEN 
			UPDATE SET
			 [type_desc] = src.[type_desc]
			,[name] = src.[name]
			,[physical_name] = src.[physical_name]
			,[state_desc] = src.[state_desc]
			,[size_mb] = src.[size_mb]
			,[max_size_mb] = src.[max_size_mb]
			,[growth_mb] = src.[growth_mb]
			,[growth_percent] = src.[growth_percent]
			,[LastUpdatedUTC] = SYSUTCDATETIME()
		WHEN NOT MATCHED BY SOURCE THEN --File was removed from the database
			UPDATE SET
			[type_desc] = N'NONE'
			,[name] = N'*removed*'
			,[physical_name] = N'*removed*'
			,[state_desc] = N'REMOVED'
			,[size_mb] = 0
			,[max_size_mb] = NULL
			,[growth_mb] = NULL
			,[growth_percent] = NULL
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
-- StoredProcedure [transfer].[databasefile_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  StoredProcedure [transfer].[databasefile_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[databasefile_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[databasefile_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[databasefile_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-06-03	Mikael Wedham		History functionality added
*******************************************************************************/
ALTER PROCEDURE [transfer].[databasefile_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

		DECLARE @fp TABLE (
		[serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[type_desc] [nvarchar](60) NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[physical_name] [nvarchar](260) NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[max_size_mb] [int] NULL,
		[growth_mb] [int] NULL,
		[growth_percent] [int] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL)


	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
	OUTPUT @serverid serverid 
	     , inserted.[database_id]
		 , inserted.[file_id]
		 , inserted.[type_desc]
		 , inserted.[name]
		 , inserted.[physical_name]
		 , inserted.[state_desc]
		 , inserted.[size_mb]
		 , inserted.[max_size_mb]
		 , inserted.[growth_mb]
		 , inserted.[growth_percent]
		 , inserted.[LastUpdatedUTC]
		 , inserted.[LastHandledUTC]
	INTO @fp
	FROM [data].[databasefile_properties] s
	WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]

    SELECT fp.serverid 
	     , fp.[database_id]
		 , fp.[file_id]
		 , fp.[type_desc]
		 , fp.[name]
		 , fp.[physical_name]
		 , fp.[state_desc]
		 , fp.[size_mb]
		 , fp.[max_size_mb]
		 , fp.[growth_mb]
		 , fp.[growth_percent]
		 , fp.[LastUpdatedUTC]
		 , fp.[LastHandledUTC]
	FROM @fp fp

END
GO

----------------------------------------------------------------
-- Finalizing [databasefile_properties]
----------------------------------------------------------------
RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'databasefile_properties', '0 */6 * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
