/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[wait_stats]
   -----------------------------------------
   Collecting wait_stats for SQL Server.
   Waits are calculated based on collection interval.
   If collection is run every 5 minutes, data is prtitioned for every
   5 minute interval. Numbers are aggregatable.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2022-04-28	Mikael Wedham		+Modified Schema of temp-tables
*******************************************************************************/
CREATE   PROCEDURE [collect].[wait_stats]
AS
BEGIN
PRINT('[collect].[wait_stats] - Collecting wait_stats for SQL Server')
SET NOCOUNT ON

DECLARE @wait_stats TABLE ([wait_type] nvarchar(60) NOT NULL
                          ,[wait_time_seconds] decimal(19,3) NOT NULL
						  ,[resource_wait_time_seconds] decimal(19,3) NOT NULL
						  ,[signal_wait_time_seconds] decimal(19,3) NOT NULL
						  ,[wait_count] bigint NOT NULL)

	INSERT INTO @wait_stats ([wait_type], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count])
	SELECT [wait_type]
		 , [wait_time_ms] / 1000.0 
		 , ([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 
		 , [signal_wait_time_ms] / 1000.0 
		 , [waiting_tasks_count] 
    FROM sys.dm_os_wait_stats
    WHERE [wait_type] NOT IN (
        N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP', N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE', 
        N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE', N'EXECSYNC', 
        N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX', N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', 
		N'MEMORY_ALLOCATION_EXT', N'ONDEMAND_TASK_QUEUE', N'PARALLEL_REDO_DRAIN_WORKER', N'PARALLEL_REDO_LOG_CACHE', N'PARALLEL_REDO_TRAN_LIST', 
		N'PARALLEL_REDO_WORKER_SYNC', N'PARALLEL_REDO_WORKER_WAIT_WORK', N'PREEMPTIVE_OS_FLUSHFILEBUFFERS', N'PREEMPTIVE_XE_GETTARGETSTATE', 
		N'PVS_PREALLOCATE', N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT', N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 
		N'QDS_ASYNC_QUEUE', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'QDS_SHUTDOWN_QUEUE', N'REDO_THREAD_PENDING_WORK', 
		N'REQUEST_FOR_DEADLOCK_SEARCH', N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'SLEEP_DCOMSTARTUP', 
		N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY', N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK', 
		N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SOS_WORK_DISPATCHER', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SQLTRACE_BUFFER_FLUSH', 
		N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES', N'VDI_CLIENT_OTHER', N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', 
		N'WAIT_XTP_RECOVERY', N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'XE_DISPATCHER_JOIN', 
		N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT' 		
		)
    AND [waiting_tasks_count] > 0

	INSERT INTO [data].[wait_stats] ([rowtime] ,[wait_type], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count], [LastUpdated])
	SELECT [rowtime] = SYSUTCDATETIME()
	, [wait_type] = w.[wait_type]
	, [wait_time_seconds] = w.[wait_time_seconds] - ISNULL(p.[wait_time_seconds], 0)
	, [resource_wait_time_seconds] = w.[resource_wait_time_seconds] - ISNULL(p.[resource_wait_time_seconds], 0)
	, [signal_wait_time_seconds] = w.[signal_wait_time_seconds] - ISNULL(p.[signal_wait_time_seconds], 0)
	, [wait_count] = w.[wait_count] - ISNULL(p.[wait_count], 0)
	, [LastUpdated] = SYSUTCDATETIME()
	FROM @wait_stats w LEFT OUTER JOIN [data_previous].[wait_stats] p
	  ON w.[wait_type] = p.[wait_type]
	WHERE (w.[wait_count] - ISNULL(p.[wait_count], 0)) > 0

	TRUNCATE TABLE [data_previous].[wait_stats]

	INSERT INTO [data_previous].[wait_stats] ([wait_type], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count])
	SELECT [wait_type], [wait_time_seconds], [resource_wait_time_seconds], [signal_wait_time_seconds], [wait_count]
	FROM @wait_stats

END