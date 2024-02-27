SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [connection_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'connection_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x01A27053B8A1C3310AB298E2161E0824061CCA85A10A2C3D2C4F311C289E98CC

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
	CREATE TABLE [data].[connection_properties](
		[db_name] [nvarchar](128) NOT NULL,
		[host_name] [nvarchar](128) NOT NULL,
		[login_name] [nvarchar](128) NOT NULL,
		[program_name] [nvarchar](128) NOT NULL,
		[connection_weight] [bigint] NOT NULL,
		[last_seen] [datetime2](0) NOT NULL
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO




RAISERROR(N'/****** Object:  StoredProcedure [collect].[connection_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[connection_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[connection_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[connection_properties]
   -----------------------------------------
   Collects information about applications and users that connect to each database.
   Does NOT catch every single login

Date		Name				Description
----------	-------------		-----------------------------------------------
2023-08-09	Mikael Wedham		+Created v1
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
2024-02-26	Mikael Wedham		+Removed time filter from selection due to UTC issues
*******************************************************************************/
ALTER PROCEDURE [collect].[connection_properties]
AS
BEGIN
PRINT('[collect].[connection_properties] - gathering database usage for SQL Server processes')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	DECLARE @previous_collection_date datetime2(7)
	SELECT @previous_collection_date = ISNULL(MAX([StartTime]), '2000-01-01')
	FROM [internal].[executionlog] 
	WHERE [collector] = N'connection_properties'

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
	VALUES (N'connection_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	IF OBJECT_ID('tempdb..#capture_db_usage') IS NOT NULL
	BEGIN
		DROP TABLE #capture_db_usage
	END

	BEGIN TRY

		SELECT event_data = CAST(xet.target_data as xml) 
		INTO #capture_db_usage
		FROM sys.dm_xe_session_targets xet INNER JOIN sys.dm_xe_sessions xes
			ON xes.address = xet.event_session_address
		WHERE xes.name = 'sqlstatscollector-connection_properties'
		  AND xet.target_name = 'ring_buffer';

		WITH ParsedXEVENT AS (
		SELECT [database_id] =  n.value('(data[@name="database_id"]/value)[1]', 'int')
			  ,[host_name] = n.value('(action[@name="client_hostname"]/value)[1]', 'nvarchar(128)')
			  ,[last_seen] = n.value('(@timestamp)[1]', 'datetime2')
			  ,[program_name] = n.value('(action[@name="client_app_name"]/value)[1]', 'nvarchar(128)')
			  ,nt_username = n.value('(action[@name="nt_username"]/value)[1]', 'nvarchar(128)')
			  ,session_nt_username = n.value('(action[@name="session_nt_username"]/value)[1]', 'nvarchar(128)')
			  ,username = n.value('(action[@name="username"]/value)[1]', 'nvarchar(128)')
		FROM (
					SELECT event_data FROM #capture_db_usage
			   ) ed
		CROSS APPLY ed.event_data.nodes('RingBufferTarget/event') q(n)
		), XEventDataStructure AS
		(
		SELECT [db_name] = DB_NAME([database_id])
			  ,[host_name]
			  ,[login_name] = [username]
			  ,[program_name] = CASE WHEN [program_name] LIKE 'SQLAgent - TSQL JobStep%' THEN (SELECT ISNULL(N'SQLAgent - Job : ' + [job_name], [program_name]) 
																							   FROM [data].[job_properties]
																							   WHERE job_id = CAST(CONVERT(binary(16), SUBSTRING([program_name], 30, 34), 1) AS uniqueidentifier))
									 ELSE [program_name] END
			  ,[connection_weight] = COUNT(*)
			  ,[last_seen] = MAX([last_seen])
		FROM ParsedXEVENT
		WHERE DB_NAME([database_id]) IS NOT NULL 
		  AND [program_name] <> 'Microsoft SQL Server Management Studio - Transact-SQL IntelliSense'
		  --AND [last_seen] >= @previous_collection_date
		GROUP BY DB_NAME([database_id]), [host_name], [username], [program_name]
		)

			MERGE [data].[connection_properties] dest
			USING (
					SELECT [db_name]
						 , [host_name]
						 , [login_name]
						 , [program_name]
						 , [connection_weight]
						 , [last_seen]
					FROM XEventDataStructure
				) src
			ON dest.[db_name] = src.[db_name] COLLATE Finnish_Swedish_CI_AS
				AND dest.[host_name] = src.[host_name] COLLATE Finnish_Swedish_CI_AS
				AND dest.[login_name] = src.[login_name] COLLATE Finnish_Swedish_CI_AS
				AND dest.[program_name] = src.[program_name] COLLATE Finnish_Swedish_CI_AS
			WHEN NOT MATCHED THEN
				INSERT ([db_name], [host_name], [login_name], [program_name], [connection_weight], [last_seen])
				VALUES (src.[db_name], src.[host_name], src.[login_name], src.[program_name], [connection_weight], src.[last_seen])
			WHEN MATCHED THEN 
				UPDATE SET dest.[last_seen] = src.[last_seen]
				, dest.[connection_weight] = dest.[connection_weight] + src.[connection_weight]
			;

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

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'usage', 'connection_properties', '*/20 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 0);
GO
