/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
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
*******************************************************************************/
CREATE PROCEDURE [collect].[cpu_stats]
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
	(SELECT TOP(60) [record_id]
				  , [SQLProcessUtilization]
				  , [SystemIdle]
				  , [OtherProcess] = 100 - [SystemIdle] - [SQLProcessUtilization] -- Only calculated for easy reporting
				  , [UTC] = CAST(DATEADD(ms, -1 * (@ts_now - [timestamp]), SYSUTCDATETIME()) as datetime2(3)) --Real datetime (UTC) from the [timestamp]
	 FROM [cpustats]
	 ORDER BY [record_id] DESC) src ON src.[record_id] = dest.[record_id]
	WHEN NOT MATCHED THEN /* Only insert values based on the record_id. Never update anything */
	   INSERT ([record_id], [idle_cpu], [sql_cpu], [other_cpu], [rowtime], [LastUpdated])
	   VALUES ([record_id], src.[SystemIdle], src.[SQLProcessUtilization], src.[OtherProcess], src.[UTC], SYSUTCDATETIME())
	;

END