SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [database_cpu_usage]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'database_cpu_usage'
DECLARE @TableDefinitionHash varbinary(32) = 0x592E310ECA1E953BBEC22094B4FA1D3FF4988C9DE276FB2F4F51D771B3E84275

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
	CREATE TABLE [data].[database_cpu_usage](
		[rowtime] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[cpu_time_ms] [decimal](18, 3) NOT NULL,
		[cpu_percent] [decimal](5, 2) NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_database_cpu_usage PRIMARY KEY CLUSTERED 
			(
				rowtime ASC,
				database_id ASC
			) ON [PRIMARY]
	) ON [PRIMARY]

END
GO

DECLARE @SchemaName nvarchar(128) = N'internal_data'
DECLARE @TableName nvarchar(128) = N'database_cpu_usage'
DECLARE @TableDefinitionHash varbinary(32) = 0x553EF615540E36C4E92FF20B0D7A49AD1DDD03CB699CD7E7AFE2996457DEBF9F

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
	CREATE TABLE [internal_data].[database_cpu_usage](
		[database_id] [int] NOT NULL,
		[cpu_time_ms] [decimal](18, 3) NOT NULL
	) ON [PRIMARY]
END
GO

RAISERROR(N'/****** Object:  StoredProcedure [collect].[database_cpu_usage] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[database_cpu_usage]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[database_cpu_usage] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[database_cpu_usage]
   -----------------------------------------
   Collecting cpu usage per database.
   cpu_time_ms is a delta-value since the last measurement.
   cpu_percent is the relative consumtion since the last measurement.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
*******************************************************************************/
ALTER PROCEDURE [collect].[database_cpu_usage]
AS
BEGIN
PRINT('[collect].[database_cpu_usage] - Collecting wait_stats for SQL Server')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
	VALUES (N'database_cpu_usage', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @database_cpu_usage TABLE ([database_id] int NOT NULL
									,[cpu_time_ms] decimal(18,3) NOT NULL)

	BEGIN TRY

		INSERT INTO @database_cpu_usage ([database_id], [cpu_time_ms])
		SELECT a.database_id
			, [cpu_time_ms] = SUM(qs.total_worker_time/1000.0) 
		FROM sys.dm_exec_query_stats qs WITH (NOLOCK)
		CROSS APPLY (SELECT database_id = CAST([value] AS int) 
					FROM sys.dm_exec_plan_attributes(qs.plan_handle)
					WHERE attribute = N'dbid') a
		WHERE [database_id] <> 32767 -- ResourceDB
		GROUP BY [database_id]

		;WITH cpu_usage AS (
		SELECT cpu.[database_id]
			, [cpu_time_ms] = cpu.[cpu_time_ms] - ISNULL(pcpu.[cpu_time_ms], 0)
		FROM @database_cpu_usage cpu LEFT OUTER JOIN [internal_data].[database_cpu_usage] pcpu
		ON cpu.database_id = pcpu.database_id)
		INSERT INTO [data].[database_cpu_usage] ([rowtime], [database_id], [cpu_time_ms], [cpu_percent], [LastUpdated])
		SELECT [rowtime] = SYSUTCDATETIME()
			, [database_id]
			, [cpu_time_ms]
			, [cpu_percent] = CAST(CASE WHEN (SUM([cpu_time_ms]) OVER()) = 0 THEN 0 ELSE ([cpu_time_ms] * 1.0 / SUM([cpu_time_ms]) OVER() * 100.0) END AS decimal(5, 2) )
			, [LastUpdated] = SYSUTCDATETIME()
		FROM cpu_usage
		WHERE [cpu_time_ms] > 0

		TRUNCATE TABLE [internal_data].[database_cpu_usage]

		INSERT INTO [internal_data].[database_cpu_usage] ([database_id] ,[cpu_time_ms])
		SELECT [database_id] ,[cpu_time_ms] FROM @database_cpu_usage

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


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[database_cpu_usage] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[database_cpu_usage]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[database_cpu_usage] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_cpu_usage]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_cpu_usage]
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
		 , inserted.[cpu_time_ms]
		 , inserted.[cpu_percent]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[database_cpu_usage] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[database_cpu_usage]
		WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())
	END

END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'database_cpu_usage', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
