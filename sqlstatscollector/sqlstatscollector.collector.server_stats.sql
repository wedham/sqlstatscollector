SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [server_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'server_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x5C1C8DCF38D4E21A86EFA7918FA96778B2AF8A4FBD75FF6AE6D9621B3224B7A1

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
	CREATE TABLE [data].[server_stats](
		[rowtime] [datetime2](7) NOT NULL,
		[user_connections] [int] NOT NULL,
		[batch_requests_sec] [int] NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_server_stats PRIMARY KEY CLUSTERED 
			(
				[rowtime] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'internal_data'
DECLARE @TableName nvarchar(128) = N'server_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x5D8BE6AB5E465AAA5B6B4F640903EB49EF8D8271E066A0AE9644339996CAB4A8

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
	CREATE TABLE [internal_data].[server_stats](
		[batch_requests_sec] [int] NOT NULL
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO



RAISERROR(N'/****** Object:  StoredProcedure [collect].[server_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[server_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[server_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[server_stats]
   -----------------------------------------
   Collecting running information and parameters on server/instance level 
   Page Life Expectancy and other Performance Monitor counters

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-21	Mikael Wedham		+Created v1
2022-04-27	Mikael Wedham		+Managed null values when initializing
                                 Moved PLE to memory collection
2022-04-28	Mikael Wedham		+Modified Schema of temp-tables
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
*******************************************************************************/
ALTER PROCEDURE [collect].[server_stats]
AS
BEGIN
PRINT('[collect].[server_stats] - Collecting running information and parameters on server')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
	VALUES (N'server_stats', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @user_connections int
	DECLARE @batch_requests_sec int
	DECLARE @previous_batch_requests_sec int
	DECLARE @batch_request_count int

	BEGIN TRY

		SELECT @user_connections = ISNULL([cntr_value] , 0)
		FROM sys.dm_os_performance_counters WITH (NOLOCK)
		WHERE [object_name] LIKE N'%General Statistics%' 
		AND [counter_name] = N'User Connections'

		SELECT @batch_requests_sec = ISNULL([cntr_value] , 0)
		FROM sys.dm_os_performance_counters WITH (NOLOCK)
		WHERE [object_name] LIKE N'%SQL Statistics%' 
		AND [counter_name] = N'Batch Requests/sec'

		SELECT @previous_batch_requests_sec = ISNULL([batch_requests_sec], 0)
		FROM [internal_data].[server_stats]

		SELECT @batch_request_count = @batch_requests_sec - @previous_batch_requests_sec

		IF @batch_request_count IS NULL OR @batch_request_count < 0 
		BEGIN
			--If counter was reset/restarted then begin with a new value
			SELECT @batch_request_count = ISNULL(@batch_requests_sec, 0)
		END

		INSERT INTO [data].[server_stats] ([user_connections], [batch_requests_sec], [rowtime], [LastUpdated])
									SELECT @user_connections , @batch_request_count, SYSUTCDATETIME(), SYSUTCDATETIME()

		TRUNCATE TABLE [internal_data].[server_stats]

		INSERT INTO [internal_data].[server_stats] ([batch_requests_sec])
		VALUES (@batch_requests_sec)

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

RAISERROR(N'/****** Object:  StoredProcedure [transfer].[server_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[server_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[server_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[server_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
*******************************************************************************/
ALTER PROCEDURE [transfer].[server_stats]
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
		 , inserted.[user_connections]
		 , inserted.[batch_requests_sec]
		 , inserted.[rowtime]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[server_stats] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[server_stats]
		WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())
	END

END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'server_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
