SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [cpu_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'cpu_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x22EDD24AEC1AF0B56AA239C443B03D4C40E4D6119688FF51751D37CC63AC7D5B

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
	CREATE TABLE [data].[cpu_stats](
		[rowtime] [datetime2](3) NOT NULL,
		[RowDate] [date] NOT NULL,
		[record_id] [int] NOT NULL,
		[idle_cpu] [int] NOT NULL,
		[sql_cpu] [int] NOT NULL,
		[other_cpu] [int] NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_cpu_stats] PRIMARY KEY CLUSTERED 
			(
				[RowDate] ASC,
				[record_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END
GO




RAISERROR(N'/****** Object:  StoredProcedure [collect].[cpu_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[cpu_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[cpu_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[cpu_stats]
   -----------------------------------------
   Collects information about the CPU usage on the server.
   Uses the default system health Extended Events trace
   Events should be collected at least once every hour to prevent gaps in the data
   CPU numbers are consolidated per minute (not configurable)

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2022-04-27	Mikael Wedham		+Brackets and naming
2023-08-09	Mikael Wedham		+Added RowDate to handle roll-over of the record_id column
*******************************************************************************/
ALTER PROCEDURE [collect].[cpu_stats]
AS
BEGIN
    PRINT('[collect].[cpu_stats] - gathering CPU usage for SQL Server process (minutely, with a maximum of 60 minutes)')
	SET NOCOUNT ON
	--Calculate the number of ticks. Needed for time conversion
	DECLARE @ts_now bigint = (SELECT [cpu_ticks]/([cpu_ticks]/[ms_ticks])FROM sys.dm_os_sys_info); 

	WITH [systemhealthresult] AS /* Get only the SystemHealth events from the XEvent trace */ 
	(	SELECT [timestamp] --Tick based time counter
			 , [record] = CONVERT(xml, [record]) --XML data
		FROM sys.dm_os_ring_buffers 
		WHERE [ring_buffer_type] = N'RING_BUFFER_SCHEDULER_MONITOR' 
		  AND [record] LIKE '%<SystemHealth>%'
	), [cpustats] AS /* Parse and convert the XML values to usable columns */
	(   SELECT [record_id] = [record].value('(./Record/@id)[1]', 'int') --The unique record_id. Used to prevent duplicates
			 , [SystemIdle] = [record].value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') --Percentage of time CPU is idle
			 , [SQLProcessUtilization] = [record].value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') --Percentage of time CPU is used by SQL Server
			 , [timestamp] --Tick based time counter
		FROM [systemhealthresult])

	MERGE [data].[cpu_stats] dest USING
	(SELECT TOP(60) [RowDate] = CAST(DATEADD(ms, -1 * (@ts_now - [timestamp]), SYSUTCDATETIME()) as date) --Real datetime (UTC) from the [timestamp]
	              , [record_id]
				  , [SQLProcessUtilization]
				  , [SystemIdle]
				  , [OtherProcess] = 100 - [SystemIdle] - [SQLProcessUtilization] -- Only calculated for easy reporting
				  , [UTC] = CAST(DATEADD(ms, -1 * (@ts_now - [timestamp]), SYSUTCDATETIME()) as datetime2(3)) --Real datetime (UTC) from the [timestamp]
	 FROM [cpustats]
	 ORDER BY [record_id] DESC) src ON src.[record_id] = dest.[record_id] AND src.[RowDate] = dest.[RowDate]
	WHEN NOT MATCHED THEN /* Only insert values based on the record_id. Never update anything */
	   INSERT ([RowDate], [record_id], [idle_cpu], [sql_cpu], [other_cpu], [rowtime], [LastUpdated])
	   VALUES (src.[RowDate], [record_id], src.[SystemIdle], src.[SQLProcessUtilization], src.[OtherProcess], src.[UTC], SYSUTCDATETIME())
	;
END
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[cpu_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[cpu_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[cpu_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[cpu_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
*******************************************************************************/
ALTER PROCEDURE [transfer].[cpu_stats]
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
	     , inserted.[record_id]
		 , inserted.[idle_cpu]
		 , inserted.[sql_cpu]
		 , inserted.[other_cpu]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[cpu_stats] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[cpu_stats]
		WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())
	END
END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'cpu_stats', '0 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01');
GO


