
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[databasefile_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [transfer].[databasefile_properties]
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
		 , inserted.[file_id]
		 , inserted.[type_desc]
		 , inserted.[name]
		 , inserted.[physical_name]
		 , inserted.[state_desc]
		 , inserted.[size_mb]
		 , inserted.[max_size_mb]
		 , inserted.[growth_mb]
		 , inserted.[growth_percent]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[databasefile_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END