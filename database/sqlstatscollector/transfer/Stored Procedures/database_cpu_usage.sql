

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_cpu_usage]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-05	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
*******************************************************************************/
CREATE PROCEDURE [transfer].[database_cpu_usage]
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
		 , inserted.[cpu_time_ms]
		 , inserted.[cpu_percent]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[database_cpu_usage] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

	DELETE FROM [data].[database_cpu_usage]
	WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())

END