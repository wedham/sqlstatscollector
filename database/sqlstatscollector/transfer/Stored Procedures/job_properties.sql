

CREATE PROCEDURE [transfer].[job_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT serverid = @serverid
	     , job_id
		 , job_name
		 , [description]
		 , job_category
		 , job_owner
		 , [enabled]
		 , notify_email_desc
		 , run_status_desc
		 , last_startdate
		 , last_duration
		 , run_duration_avg
		 , LastUpdated
		 , LastHandled
	FROM [data].[job_properties]
END