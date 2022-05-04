
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[availability_group_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-04	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [transfer].[availability_group_properties]
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
	     , inserted.[group_id]
		 , inserted.[name]
		 , inserted.[primary_replica]
		 , inserted.[recovery_health_desc]
		 , inserted.[synchronization_health_desc]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[availability_group_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END