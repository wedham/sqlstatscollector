CREATE PROCEDURE [transfer].[cpu_stats]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT serverid = @serverid
	     , record_id
		 , idle_cpu
		 , sql_cpu
		 , other_cpu
		 , rowtime
		 , LastUpdated
		 , LastHandled
	FROM [data].[cpu_stats]
END