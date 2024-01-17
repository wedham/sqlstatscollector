SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [database_memory_usage]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'database_memory_usage'
DECLARE @TableDefinitionHash varbinary(32) = 0x840AD75FCB6880741EA52ADFE4F946CEBC965D7560B89714264F20C6214CEEFF

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
	CREATE TABLE [data].[database_memory_usage](
		[rowtime] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[page_count] [int] NOT NULL,
		[cached_size_mb] [decimal](15, 2) NOT NULL,
		[buffer_pool_percent] [decimal](5, 2) NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_database_memory_usage PRIMARY KEY CLUSTERED 
			(
				rowtime ASC,
				database_id ASC
			) ON [PRIMARY]
	) ON [PRIMARY]
END



RAISERROR(N'/****** Object:  StoredProcedure [collect].[database_memory_usage] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[database_memory_usage]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[database_memory_usage] AS SELECT NULL')
END
GO

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[database_memory_usage]
   -----------------------------------------
   Collecting memory usage per database.
   cpu_time_ms is a delta-value since the last measurement.
   cpu_percent is the relative consumtion since the last measurement.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [collect].[database_memory_usage]
AS
BEGIN
PRINT('[collect].[database_memory_usage] - Collecting wait_stats for SQL Server')
SET NOCOUNT ON

	;WITH memory_usage AS
	(SELECT [database_id]
		  , [page_count] = COUNT(page_id) 
		  , [cached_size_mb] = CAST(COUNT_BIG(*) * 8/1024.0 AS decimal(15,2))
	FROM sys.dm_os_buffer_descriptors WITH (NOLOCK)
	GROUP BY database_id)

	INSERT INTO [data].[database_memory_usage]  ([rowtime] ,[database_id] ,[page_count] ,[cached_size_mb] , [buffer_pool_percent], [LastUpdated])
	SELECT [rowtime] = SYSUTCDATETIME()
		 , [database_id]
		 , [page_count]
		 , [cached_size_mb] 
		 , [buffer_pool_percent] = CAST([cached_size_mb] / SUM([cached_size_mb]) OVER() * 100.0 AS decimal(5,2))
		 , [LastUpdated] = SYSUTCDATETIME()
	FROM memory_usage
	WHERE [database_id] <> 32767 -- ResourceDB
	  AND [page_count] > 0
	;

END
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[database_memory_usage] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[database_memory_usage]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[database_memory_usage] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_memory_usage]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_memory_usage]
(@cleanup bit = 0)
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
	     , inserted.[rowtime]
		 , inserted.[database_id]
		 , inserted.[page_count]
		 , inserted.[cached_size_mb]
		 , inserted.[buffer_pool_percent]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[database_memory_usage] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[database_memory_usage]
		WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())
	END

END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'database_memory_usage', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01');
GO
