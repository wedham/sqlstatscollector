

CREATE PROCEDURE [transfer].[server_stats]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT serverid = @serverid
	     , page_life_expectancy
		 , user_connections
		 , batch_requests_sec
		 , rowtime
		 , LastHandled
	FROM [data].[server_stats]
END