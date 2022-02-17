/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[memory_stats]
   -----------------------------------------
   Collects information about the memory usage on the server.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-02-04	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [collect].[memory_stats]
AS
BEGIN
    PRINT('[collect].[memory_stats] - gathering memory usage ')

	SELECT total_physical_memory_kb/1024 AS [Physical Memory (MB)], 
       available_physical_memory_kb/1024 AS [Available Memory (MB)], 
       total_page_file_kb/1024 AS [Page File Commit Limit (MB)],
	   total_page_file_kb/1024 - total_physical_memory_kb/1024 AS [Physical Page File Size (MB)],
	   available_page_file_kb/1024 AS [Available Page File (MB)], 
	   system_cache_kb/1024 AS [System Cache (MB)],
       system_memory_state_desc AS [System Memory State]
FROM sys.dm_os_sys_memory WITH (NOLOCK) OPTION (RECOMPILE);
------

END