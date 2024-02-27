SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [wait_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'wait_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xA2F127794469F3162EF39E2070A04DE25D259BC1E0D565886CAF2570025BF96A

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
	CREATE TABLE [data].[wait_stats](
		[rowtime] [datetime2](7) NOT NULL,
		[wait_type] [nvarchar](127) NOT NULL,
		[interval_percentage] [decimal](18, 3) NOT NULL,
		[wait_time_seconds] [decimal](18, 3) NOT NULL,
		[resource_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[signal_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[wait_count] [bigint] NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_wait_stats PRIMARY KEY CLUSTERED 
			(
				rowtime ASC,
				wait_type ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'internal_data'
DECLARE @TableName nvarchar(128) = N'wait_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xD48D81E243F6110F0E89B61E42A5807CBBB71D35E4C87474565F125C5CC4F5C8

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
	CREATE TABLE [internal_data].[wait_stats](
		[wait_type] [nvarchar](127) NOT NULL,
		[wait_time_seconds] [decimal](18, 3) NOT NULL,
		[resource_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[signal_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[wait_count] [bigint] NOT NULL,
		CONSTRAINT PK_internal_data_wait_stats PRIMARY KEY CLUSTERED 
			(
				wait_type ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO


RAISERROR(N'/****** Object:  StoredProcedure [collect].[wait_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[wait_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[wait_stats] AS SELECT NULL')
END
GO

/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[wait_stats]
   -----------------------------------------
   Collecting wait_stats for SQL Server.
   Waits are calculated based on collection interval.
   If collection is run every 5 minutes, data is prtitioned for every
   5 minute interval. Numbers are aggregatable.

   Inspiration and code taken from Paul Randal / sqlskills.com
   https://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/
   Last updated October 1, 2021

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2022-04-28	Mikael Wedham		+Modified Schema of temp-tables
2022-05-05     Mikael Wedham       +Code and events updated from sqlskills website.
                                    Credits and help links added.
2024-01-17     Mikael Wedham		+Added division by zero handling
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
*******************************************************************************/
ALTER PROCEDURE [collect].[wait_stats]
AS
BEGIN
PRINT('[collect].[wait_stats] - Collecting wait_stats for SQL Server')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
	VALUES (N'wait_stats', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @wait_stats TABLE ([wait_type] nvarchar(60) NOT NULL
							,[wait_time_seconds] decimal(19,3) NOT NULL
							,[resource_wait_time_seconds] decimal(19,3) NOT NULL
							,[signal_wait_time_seconds] decimal(19,3) NOT NULL
							,[wait_count] bigint NOT NULL)

	BEGIN TRY

		INSERT INTO @wait_stats ([wait_type], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count])
		SELECT [wait_type]
			, [wait_time_ms] / 1000.0 
			, ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 
			, [signal_wait_time_ms] / 1000.0 
			, [waiting_tasks_count] 
		FROM sys.dm_os_wait_stats
		--This section should be checked when a new update on sqlskills.com is available
		--BEGIN SECTION
		WHERE [wait_type] NOT IN (
			-- These wait types are almost 100% never a problem and so they are
			-- filtered out to avoid them skewing the results. Click on the URL
			-- for more information.
			N'BROKER_EVENTHANDLER', -- https://www.sqlskills.com/help/waits/BROKER_EVENTHANDLER
			N'BROKER_RECEIVE_WAITFOR', -- https://www.sqlskills.com/help/waits/BROKER_RECEIVE_WAITFOR
			N'BROKER_TASK_STOP', -- https://www.sqlskills.com/help/waits/BROKER_TASK_STOP
			N'BROKER_TO_FLUSH', -- https://www.sqlskills.com/help/waits/BROKER_TO_FLUSH
			N'BROKER_TRANSMITTER', -- https://www.sqlskills.com/help/waits/BROKER_TRANSMITTER
			N'CHECKPOINT_QUEUE', -- https://www.sqlskills.com/help/waits/CHECKPOINT_QUEUE
			N'CHKPT', -- https://www.sqlskills.com/help/waits/CHKPT
			N'CLR_AUTO_EVENT', -- https://www.sqlskills.com/help/waits/CLR_AUTO_EVENT
			N'CLR_MANUAL_EVENT', -- https://www.sqlskills.com/help/waits/CLR_MANUAL_EVENT
			N'CLR_SEMAPHORE', -- https://www.sqlskills.com/help/waits/CLR_SEMAPHORE
	
			-- Maybe comment this out if you have parallelism issues
			N'CXCONSUMER', -- https://www.sqlskills.com/help/waits/CXCONSUMER
	
			-- Maybe comment these four out if you have mirroring issues
			N'DBMIRROR_DBM_EVENT', -- https://www.sqlskills.com/help/waits/DBMIRROR_DBM_EVENT
			N'DBMIRROR_EVENTS_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_EVENTS_QUEUE
			N'DBMIRROR_WORKER_QUEUE', -- https://www.sqlskills.com/help/waits/DBMIRROR_WORKER_QUEUE
			N'DBMIRRORING_CMD', -- https://www.sqlskills.com/help/waits/DBMIRRORING_CMD
			N'DIRTY_PAGE_POLL', -- https://www.sqlskills.com/help/waits/DIRTY_PAGE_POLL
			N'DISPATCHER_QUEUE_SEMAPHORE', -- https://www.sqlskills.com/help/waits/DISPATCHER_QUEUE_SEMAPHORE
			N'EXECSYNC', -- https://www.sqlskills.com/help/waits/EXECSYNC
			N'FSAGENT', -- https://www.sqlskills.com/help/waits/FSAGENT
			N'FT_IFTS_SCHEDULER_IDLE_WAIT', -- https://www.sqlskills.com/help/waits/FT_IFTS_SCHEDULER_IDLE_WAIT
			N'FT_IFTSHC_MUTEX', -- https://www.sqlskills.com/help/waits/FT_IFTSHC_MUTEX
	
		-- Maybe comment these six out if you have AG issues
			N'HADR_CLUSAPI_CALL', -- https://www.sqlskills.com/help/waits/HADR_CLUSAPI_CALL
			N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', -- https://www.sqlskills.com/help/waits/HADR_FILESTREAM_IOMGR_IOCOMPLETION
			N'HADR_LOGCAPTURE_WAIT', -- https://www.sqlskills.com/help/waits/HADR_LOGCAPTURE_WAIT
			N'HADR_NOTIFICATION_DEQUEUE', -- https://www.sqlskills.com/help/waits/HADR_NOTIFICATION_DEQUEUE
			N'HADR_TIMER_TASK', -- https://www.sqlskills.com/help/waits/HADR_TIMER_TASK
			N'HADR_WORK_QUEUE', -- https://www.sqlskills.com/help/waits/HADR_WORK_QUEUE
	
			N'KSOURCE_WAKEUP', -- https://www.sqlskills.com/help/waits/KSOURCE_WAKEUP
			N'LAZYWRITER_SLEEP', -- https://www.sqlskills.com/help/waits/LAZYWRITER_SLEEP
			N'LOGMGR_QUEUE', -- https://www.sqlskills.com/help/waits/LOGMGR_QUEUE
			N'MEMORY_ALLOCATION_EXT', -- https://www.sqlskills.com/help/waits/MEMORY_ALLOCATION_EXT
			N'ONDEMAND_TASK_QUEUE', -- https://www.sqlskills.com/help/waits/ONDEMAND_TASK_QUEUE
			N'PARALLEL_REDO_DRAIN_WORKER', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_DRAIN_WORKER
			N'PARALLEL_REDO_LOG_CACHE', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_LOG_CACHE
			N'PARALLEL_REDO_TRAN_LIST', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_TRAN_LIST
			N'PARALLEL_REDO_WORKER_SYNC', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_SYNC
			N'PARALLEL_REDO_WORKER_WAIT_WORK', -- https://www.sqlskills.com/help/waits/PARALLEL_REDO_WORKER_WAIT_WORK
			N'PREEMPTIVE_OS_FLUSHFILEBUFFERS', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_OS_FLUSHFILEBUFFERS
			N'PREEMPTIVE_XE_GETTARGETSTATE', -- https://www.sqlskills.com/help/waits/PREEMPTIVE_XE_GETTARGETSTATE
			N'PVS_PREALLOCATE', -- https://www.sqlskills.com/help/waits/PVS_PREALLOCATE
			N'PWAIT_ALL_COMPONENTS_INITIALIZED', -- https://www.sqlskills.com/help/waits/PWAIT_ALL_COMPONENTS_INITIALIZED
			N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', -- https://www.sqlskills.com/help/waits/PWAIT_DIRECTLOGCONSUMER_GETNEXT
			N'PWAIT_EXTENSIBILITY_CLEANUP_TASK', -- https://www.sqlskills.com/help/waits/PWAIT_EXTENSIBILITY_CLEANUP_TASK
			N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', -- https://www.sqlskills.com/help/waits/QDS_PERSIST_TASK_MAIN_LOOP_SLEEP
			N'QDS_ASYNC_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_ASYNC_QUEUE
			N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
				-- https://www.sqlskills.com/help/waits/QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP
			N'QDS_SHUTDOWN_QUEUE', -- https://www.sqlskills.com/help/waits/QDS_SHUTDOWN_QUEUE
			N'REDO_THREAD_PENDING_WORK', -- https://www.sqlskills.com/help/waits/REDO_THREAD_PENDING_WORK
			N'REQUEST_FOR_DEADLOCK_SEARCH', -- https://www.sqlskills.com/help/waits/REQUEST_FOR_DEADLOCK_SEARCH
			N'RESOURCE_QUEUE', -- https://www.sqlskills.com/help/waits/RESOURCE_QUEUE
			N'SERVER_IDLE_CHECK', -- https://www.sqlskills.com/help/waits/SERVER_IDLE_CHECK
			N'SLEEP_BPOOL_FLUSH', -- https://www.sqlskills.com/help/waits/SLEEP_BPOOL_FLUSH
			N'SLEEP_DBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DBSTARTUP
			N'SLEEP_DCOMSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_DCOMSTARTUP
			N'SLEEP_MASTERDBREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERDBREADY
			N'SLEEP_MASTERMDREADY', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERMDREADY
			N'SLEEP_MASTERUPGRADED', -- https://www.sqlskills.com/help/waits/SLEEP_MASTERUPGRADED
			N'SLEEP_MSDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_MSDBSTARTUP
			N'SLEEP_SYSTEMTASK', -- https://www.sqlskills.com/help/waits/SLEEP_SYSTEMTASK
			N'SLEEP_TASK', -- https://www.sqlskills.com/help/waits/SLEEP_TASK
			N'SLEEP_TEMPDBSTARTUP', -- https://www.sqlskills.com/help/waits/SLEEP_TEMPDBSTARTUP
			N'SNI_HTTP_ACCEPT', -- https://www.sqlskills.com/help/waits/SNI_HTTP_ACCEPT
			N'SOS_WORK_DISPATCHER', -- https://www.sqlskills.com/help/waits/SOS_WORK_DISPATCHER
			N'SP_SERVER_DIAGNOSTICS_SLEEP', -- https://www.sqlskills.com/help/waits/SP_SERVER_DIAGNOSTICS_SLEEP
			N'SQLTRACE_BUFFER_FLUSH', -- https://www.sqlskills.com/help/waits/SQLTRACE_BUFFER_FLUSH
			N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', -- https://www.sqlskills.com/help/waits/SQLTRACE_INCREMENTAL_FLUSH_SLEEP
			N'SQLTRACE_WAIT_ENTRIES', -- https://www.sqlskills.com/help/waits/SQLTRACE_WAIT_ENTRIES
			N'VDI_CLIENT_OTHER', -- https://www.sqlskills.com/help/waits/VDI_CLIENT_OTHER
			N'WAIT_FOR_RESULTS', -- https://www.sqlskills.com/help/waits/WAIT_FOR_RESULTS
			N'WAITFOR', -- https://www.sqlskills.com/help/waits/WAITFOR
			N'WAITFOR_TASKSHUTDOWN', -- https://www.sqlskills.com/help/waits/WAITFOR_TASKSHUTDOWN
			N'WAIT_XTP_RECOVERY', -- https://www.sqlskills.com/help/waits/WAIT_XTP_RECOVERY
			N'WAIT_XTP_HOST_WAIT', -- https://www.sqlskills.com/help/waits/WAIT_XTP_HOST_WAIT
			N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', -- https://www.sqlskills.com/help/waits/WAIT_XTP_OFFLINE_CKPT_NEW_LOG
			N'WAIT_XTP_CKPT_CLOSE', -- https://www.sqlskills.com/help/waits/WAIT_XTP_CKPT_CLOSE
			N'XE_DISPATCHER_JOIN', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_JOIN
			N'XE_DISPATCHER_WAIT', -- https://www.sqlskills.com/help/waits/XE_DISPATCHER_WAIT
			N'XE_TIMER_EVENT' -- https://www.sqlskills.com/help/waits/XE_TIMER_EVENT
			)
			--END SECTION
			
			AND [waiting_tasks_count] > 0
		
		INSERT INTO [data].[wait_stats] ([rowtime] ,[wait_type], [interval_percentage], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count], [LastUpdated])
		SELECT [rowtime] = SYSUTCDATETIME()
			, [wait_type]
			, [percentage]
			, [wait_time_seconds]
			, [resource_wait_time_seconds]
			, [signal_wait_time_seconds]
			, [wait_count]
			, [LastUpdated] = SYSUTCDATETIME()
		FROM (
				SELECT [wait_type] = w.[wait_type]
				, [percentage] = CAST( 100.0 * (w.[wait_time_seconds] - ISNULL(p.[wait_time_seconds], 0)) 
										/ 
										CASE WHEN SUM( w.[wait_time_seconds] - ISNULL(p.[wait_time_seconds], 0)) OVER() = 0 THEN 1
											ELSE SUM( w.[wait_time_seconds] - ISNULL(p.[wait_time_seconds], 0)) OVER()  END
											AS decimal(18,3))
				, [wait_time_seconds] = w.[wait_time_seconds] - ISNULL(p.[wait_time_seconds], 0)
				, [resource_wait_time_seconds] = w.[resource_wait_time_seconds] - ISNULL(p.[resource_wait_time_seconds], 0)
				, [signal_wait_time_seconds] = w.[signal_wait_time_seconds] - ISNULL(p.[signal_wait_time_seconds], 0)
				, [wait_count] = w.[wait_count] - ISNULL(p.[wait_count], 0)
				FROM @wait_stats w LEFT OUTER JOIN [internal_data].[wait_stats] p
				ON w.[wait_type] = p.[wait_type]
				WHERE (w.[wait_count] - ISNULL(p.[wait_count], 0)) > 0
			) deltawaits
		WHERE deltawaits.[percentage] > 2

		TRUNCATE TABLE [internal_data].[wait_stats]

		INSERT INTO [internal_data].[wait_stats] ([wait_type], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count])
		SELECT [wait_type], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count]
		FROM @wait_stats

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


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[wait_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[wait_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[wait_stats] AS SELECT NULL')
END
GO

/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[wait_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
2024-01-17	Mikael Wedham		Refreshed filter on wait types
*******************************************************************************/
ALTER PROCEDURE [transfer].[wait_stats]
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
		 , inserted.[wait_type]
		 , inserted.[interval_percentage]
		 , inserted.[wait_time_seconds]
		 , inserted.[resource_wait_time_seconds]
		 , inserted.[signal_wait_time_seconds]
		 , inserted.[wait_count]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[wait_stats] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]
	
	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[wait_stats]
		WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())
	END
END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'wait_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
