SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [memory_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'memory_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x05B12D7EDE511EF5A392D946CC44B7F4375D7820BABD7F4B29BBF327B5A335BD

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
	CREATE TABLE [data].[memory_stats](
		[rowtimeutc] [datetime2](7) NOT NULL,
		[page_life_expectancy] [int] NOT NULL,
		[target_server_memory_mb] [bigint] NOT NULL,
		[total_server_memory_mb] [bigint] NOT NULL,
		[total_physical_memory_mb] [bigint] NOT NULL,
		[available_physical_memory_mb] [bigint] NOT NULL,
		[percent_memory_used] [decimal](18, 3) NOT NULL,
		[system_memory_state_desc] [nvarchar](256) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_memory_stats PRIMARY KEY CLUSTERED 
			(
				rowtimeutc ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


RAISERROR(N'/****** Object:  StoredProcedure [collect].[memory_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[memory_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[memory_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[memory_stats]
   -----------------------------------------
   Collects information about the memory usage on the server.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-27	Mikael Wedham		+Created v1
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
2026-03-31	Mikael Wedham		Adding UTC to column names
*******************************************************************************/
ALTER PROCEDURE [collect].[memory_stats]
AS
BEGIN
PRINT('[collect].[memory_stats] - gathering memory usage ')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'memory_stats', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @page_life_expectancy int
	DECLARE @total_server_memory_mb bigint
	DECLARE @target_server_memory_mb bigint

	BEGIN TRY

		SELECT @page_life_expectancy = ISNULL([cntr_value] , 0)
		FROM sys.dm_os_performance_counters WITH (NOLOCK)
		WHERE [object_name] LIKE N'%Buffer Manager%' 
		AND [counter_name] = N'Page life expectancy'
	
		SELECT @total_server_memory_mb = ISNULL([cntr_value] , 0) / (1024)
		FROM sys.dm_os_performance_counters WITH (NOLOCK)
		WHERE [object_name] LIKE N'%Memory Manager%' 
		AND [counter_name] = N'Total Server Memory (KB)'

		SELECT @target_server_memory_mb = ISNULL([cntr_value] , 0) / (1024)
		FROM sys.dm_os_performance_counters WITH (NOLOCK)
		WHERE [object_name] LIKE N'%Memory Manager%' 
		AND [counter_name] = N'Target Server Memory (KB)'

		INSERT INTO [data].[memory_stats] ([page_life_expectancy], [target_server_memory_mb], [total_server_memory_mb], [total_physical_memory_mb], [available_physical_memory_mb], [percent_memory_used], [system_memory_state_desc], [rowtimeutc], [LastUpdatedUTC])
		SELECT [page_life_expectancy] = @page_life_expectancy
			, [target_sql_server_memory_mb] = @target_server_memory_mb
			, [total_sql_server_memory_mb] = @total_server_memory_mb
			, [total_physical_memory_mb] = [total_physical_memory_kb]/1024 
			, [available_physical_memory_mb] = [available_physical_memory_kb]/1024
			, [percent_physical_memory_used] = CAST(100 - (100 * CAST([available_physical_memory_kb] AS decimal(18,3))/CAST([total_physical_memory_kb] AS decimal(18,3))) as decimal(18,3))
			, [system_memory_state] = [system_memory_state_desc]
			, SYSUTCDATETIME()
			, SYSUTCDATETIME()
		FROM sys.dm_os_sys_memory WITH (NOLOCK) 

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


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[memory_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[memory_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[memory_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[memory_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
2026-03-31	Mikael Wedham		Adding UTC to column names
*******************************************************************************/
ALTER PROCEDURE [transfer].[memory_stats]
(@cleanup bit = 0)
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
	OUTPUT @serverid serverid 
		 , inserted.[rowtimeutc]
	     , inserted.[page_life_expectancy]
	     , inserted.[target_server_memory_mb]
	     , inserted.[total_server_memory_mb]
	     , inserted.[total_physical_memory_mb]
	     , inserted.[available_physical_memory_mb]
	     , inserted.[percent_memory_used]
	     , inserted.[system_memory_state_desc]
		 , inserted.[LastUpdatedUTC]
		 , inserted.[LastHandledUTC]
	FROM [data].[memory_stats] s
	WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]

	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[memory_stats]
		WHERE [LastHandledUTC] < DATEADD(DAY, -7, GETDATE())
	END

END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'memory_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
