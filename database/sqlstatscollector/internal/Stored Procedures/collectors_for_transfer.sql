
CREATE PROCEDURE [internal].[collectors_for_transfer]
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT [collector] = '[' + [collector] + ']'
	     , [serverid] = @serverid
    FROM [internal].[collectors]
	WHERE [collector] NOT IN ('job_stats')
END