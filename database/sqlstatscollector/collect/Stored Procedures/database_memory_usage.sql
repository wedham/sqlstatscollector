/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[database_memory_usage]
   -----------------------------------------
   Collecting memory usage per database.
   cpu_time_ms is a delta-value since the last measurement.
   cpu_percent is the relative consumtion since the last measurement.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   PROCEDURE [collect].[database_memory_usage]
AS
BEGIN
PRINT('[collect].[database_memory_usage] - Collecting wait_stats for SQL Server')
SET NOCOUNT ON


	;WITH memory_usage AS
	(SELECT [database_id]
		  , [page_count] = COUNT(page_id) 
		  , [cached_size_mb] = CAST(COUNT_BIG(*) * 8/1024.0 AS decimal(15,2))
	FROM sys.dm_os_buffer_descriptors WITH (NOLOCK)
	GROUP BY database_id)

	INSERT INTO [data].[database_memory_usage]  ([rowtime] ,[database_id] ,[page_count] ,[cached_size_mb] , [buffer_pool_percent], [LastUpdated])
	SELECT [rowtime] = SYSUTCDATETIME()
		 , [database_id]
		 , [page_count]
		 , [cached_size_mb] 
		 , [buffer_pool_percent] = CAST([cached_size_mb] / SUM([cached_size_mb]) OVER() * 100.0 AS decimal(5,2))
		 , [LastUpdated] = SYSUTCDATETIME()
	FROM memory_usage
	WHERE [database_id] <> 32767 -- ResourceDB
	  AND [page_count] > 0
	;

END