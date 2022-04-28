
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[job_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [transfer].[job_stats]
AS
BEGIN
	SET NOCOUNT ON
	--DECLARE @serverid uniqueidentifier
	--SELECT @serverid = [serverid]
	--FROM [data].[server_properties]
	--WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	--UPDATE s
	--SET [LastHandled] = SYSUTCDATETIME()
	--OUTPUT @serverid serverid 
	--     , inserted./* columns */
	--FROM [data].[job_stats] s
	--WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]
END