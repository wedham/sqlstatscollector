/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[memory_stats]
   -----------------------------------------
   Collects information about the memory usage on the server.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-27	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [collect].[memory_stats]
AS
BEGIN
    PRINT('[collect].[memory_stats] - gathering memory usage ')
	SET NOCOUNT ON

	DECLARE @page_life_expectancy int
	DECLARE @total_server_memory_mb bigint
	DECLARE @target_server_memory_mb bigint

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

	INSERT INTO [data].[memory_stats] ([page_life_expectancy], [target_server_memory_mb], [total_server_memory_mb], [total_physical_memory_mb], [available_physical_memory_mb], [percent_memory_used], [system_memory_state_desc], [rowtime], [LastUpdated])
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
	
END