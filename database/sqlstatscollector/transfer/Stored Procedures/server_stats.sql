CREATE   PROCEDURE [transfer].[server_stats]
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
	     , inserted.[page_life_expectancy]
		 , inserted.[user_connections]
		 , inserted.[batch_requests_sec]
		 , inserted.[rowtime]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[server_stats] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END