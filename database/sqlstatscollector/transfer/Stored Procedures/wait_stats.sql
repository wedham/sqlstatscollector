

CREATE PROCEDURE [transfer].[wait_stats]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT serverid = @serverid
	     , rowtime
		 , wait_type
		 , wait_time_seconds
		 , resource_wait_time_seconds
		 , signal_wait_time_seconds
		 , wait_count
		 , LastHandled
	FROM [data].[wait_stats]
END