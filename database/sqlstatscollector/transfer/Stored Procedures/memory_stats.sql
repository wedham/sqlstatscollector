

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[memory_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [transfer].[memory_stats]
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
	     , inserted.[page_life_expectancy]
	     , inserted.[target_server_memory_mb]
	     , inserted.[total_server_memory_mb]
	     , inserted.[total_physical_memory_mb]
	     , inserted.[available_physical_memory_mb]
	     , inserted.[percent_memory_used]
	     , inserted.[system_memory_state_desc]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[memory_stats] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END