
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
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
CREATE PROCEDURE [transfer].[cpu_stats]
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

	DELETE FROM [data].[cpu_stats]
	WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())

END