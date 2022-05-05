/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[database_cpu_usage]
   -----------------------------------------
   Collecting cpu usage per database.
   cpu_time_ms is a delta-value since the last measurement.
   cpu_percent is the relative consumtion since the last measurement.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   PROCEDURE [collect].[database_cpu_usage]
AS
BEGIN
PRINT('[collect].[database_cpu_usage] - Collecting wait_stats for SQL Server')
SET NOCOUNT ON

DECLARE @database_cpu_usage TABLE ([database_id] int NOT NULL
                                  ,[cpu_time_ms] decimal(18,3) NOT NULL)

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
	FROM @database_cpu_usage cpu LEFT OUTER JOIN [data_previous].[database_cpu_usage] pcpu
	  ON cpu.database_id = pcpu.database_id)
	INSERT INTO [data].[database_cpu_usage] ([rowtime], [database_id], [cpu_time_ms], [cpu_percent], [LastUpdated])
	SELECT [rowtime] = SYSUTCDATETIME()
		 , [database_id]
		 , [cpu_time_ms]
		 , [cpu_percent] = CAST(CASE WHEN (SUM([cpu_time_ms]) OVER()) = 0 THEN 0 ELSE ([cpu_time_ms] * 1.0 / SUM([cpu_time_ms]) OVER() * 100.0) END AS decimal(5, 2) )
		 , [LastUpdated] = SYSUTCDATETIME()
	FROM cpu_usage
	WHERE [cpu_time_ms] > 0

	TRUNCATE TABLE [data_previous].[database_cpu_usage]

	INSERT INTO [data_previous].[database_cpu_usage] ([database_id] ,[cpu_time_ms])
	SELECT [database_id] ,[cpu_time_ms] FROM @database_cpu_usage

END