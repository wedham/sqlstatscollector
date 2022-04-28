
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [transfer].[database_properties]
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
	     , inserted.[database_id]
		 , inserted.[name]
		 , inserted.[owner_sid]
		 , inserted.[create_date]
		 , inserted.[compatibility_level]
		 , inserted.[collation_name]
		 , inserted.[is_auto_close_on]
		 , inserted.[is_auto_shrink_on]
		 , inserted.[state_desc]
		 , inserted.[recovery_model_desc]
		 , inserted.[page_verify_option_desc]
		 , inserted.[LastFullBackupTime]
		 , inserted.[LastDiffBackupTime]
		 , inserted.[LastLogBackupTime]
		 , inserted.[LastKnownGoodDBCCTime]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[database_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END