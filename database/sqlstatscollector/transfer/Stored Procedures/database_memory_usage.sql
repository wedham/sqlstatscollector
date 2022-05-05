

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_memory_usage]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [transfer].[database_memory_usage]
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
		 , inserted.[database_id]
		 , inserted.[page_count]
		 , inserted.[cached_size_mb]
		 , inserted.[buffer_pool_percent]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[database_memory_usage] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END