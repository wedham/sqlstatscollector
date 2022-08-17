
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[wait_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
*******************************************************************************/
CREATE PROCEDURE [transfer].[wait_stats]
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
		 , inserted.[wait_type]
		 , inserted.[interval_percentage]
		 , inserted.[wait_time_seconds]
		 , inserted.[resource_wait_time_seconds]
		 , inserted.[signal_wait_time_seconds]
		 , inserted.[wait_count]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[wait_stats] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]
	
	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[wait_stats]
		WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())
	END
END