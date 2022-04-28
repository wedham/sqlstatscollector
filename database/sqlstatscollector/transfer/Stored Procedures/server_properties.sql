
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[server_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [transfer].[server_properties]
AS
BEGIN
	SET NOCOUNT ON

	UPDATE s
	SET [LastHandled] = SYSUTCDATETIME()
	OUTPUT inserted.[serverid] 
	     , inserted.[MachineName]
		 , inserted.[ServerName]
		 , inserted.[Instance]
		 , inserted.[ComputerNamePhysicalNetBIOS]
		 , inserted.[Edition]
		 , inserted.[ProductLevel]
		 , inserted.[ProductVersion]
		 , inserted.[Collation]
		 , inserted.[IsClustered]
		 , inserted.[IsIntegratedSecurityOnly]
		 , inserted.[FilestreamConfiguredLevel]
		 , inserted.[IsHadrEnabled]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[server_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]
	AND [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

END