SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


RAISERROR(N'Collector: [databasefile_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'databasefile_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x3CE63596CF0A16CFDF4B3344FEAEB8D67392124B4BD1566213BDEC952BD72CAC

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
	RAISERROR(N'DROP original table', 10, 1) WITH NOWAIT
	SELECT @cmd = N'DROP TABLE ' + @FullName
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
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_databasefile_properties] PRIMARY KEY CLUSTERED 
			(
				[database_id] ASC,
				[file_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END
GO

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
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
	VALUES (N'databasefile_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	BEGIN TRY

		/* Only one statement is needed */
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
				, [size_mb], [max_size_mb] ,[growth_mb] ,[growth_percent] ,[LastUpdated]) 
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
			,[LastUpdated] = SYSUTCDATETIME()
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
			,[LastUpdated] = SYSUTCDATETIME();

	END TRY
	BEGIN CATCH
		DECLARE @msg nvarchar(4000)
		SELECT @error = ERROR_NUMBER(), @msg = ERROR_MESSAGE()
		PRINT (@msg)
	END CATCH

	SELECT @current_end = SYSUTCDATETIME()
	UPDATE [internal].[executionlog]
	SET [EndTime] = @current_end
	, [Duration_ms] =  ((CAST(DATEDIFF(S, @current_start, @current_end) AS bigint) * 1000000) + (DATEPART(MCS, @current_end)-DATEPART(MCS, @current_start))) / 1000.0
	, [errornumber] = @@ERROR
	WHERE [Id] = @current_logitem


END
GO



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
*******************************************************************************/
ALTER PROCEDURE [transfer].[databasefile_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	UPDATE s
	SET [LastHandled] = SYSUTCDATETIME()
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
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[databasefile_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END
GO

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
