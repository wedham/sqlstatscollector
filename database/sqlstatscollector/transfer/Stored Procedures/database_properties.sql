

CREATE PROCEDURE [transfer].[database_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT serverid = @serverid
	     , database_id
		 , [name]
		 , owner_sid
		 , create_date
		 , [compatibility_level]
		 , collation_name
		 , is_auto_close_on
		 , is_auto_shrink_on
		 , state_desc
		 , recovery_model_desc
		 , page_verify_option_desc
		 , LastFullBackupTime
		 , LastDiffBackupTime
		 , LastLogBackupTime
		 , LastKnownGoodDBCCTime
		 , LastUpdated
		 , LastHandled
	FROM [data].[database_properties]
END