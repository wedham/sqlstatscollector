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
DECLARE @TableDefinitionHash varbinary(32) = 0xB3F39C7A784CAECF1D8A036F62659DDA59234D47DDE8139EA0E14DE1A098F0DA

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
GO

DECLARE @SchemaName nvarchar(128) = N'internal_data'
DECLARE @TableName nvarchar(128) = N'server_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xBEA40ADC7BED60658E0F145C8B1293B1769D0708D73F9EA212C1963C8952F18C

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
*******************************************************************************/
ALTER PROCEDURE [collect].[server_stats]
AS
BEGIN
PRINT('[collect].[server_stats] - Collecting running information and parameters on server')
SET NOCOUNT ON

	DECLARE @user_connections int
	DECLARE @batch_requests_sec int
	DECLARE @previous_batch_requests_sec int
	DECLARE @batch_request_count int

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
		INSERT ([section], [collector], [cron], [lastrun])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01');
GO
