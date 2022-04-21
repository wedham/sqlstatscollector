

CREATE PROCEDURE [transfer].[server_properties]
AS
BEGIN
	SET NOCOUNT ON

	SELECT serverid
	     , MachineName
		 , ServerName
		 , Instance
		 , ComputerNamePhysicalNetBIOS
		 , Edition
		 , ProductLevel
		 , ProductVersion
		 , Collation
		 , IsClustered
		 , IsIntegratedSecurityOnly
		 , FilestreamConfiguredLevel
		 , IsHadrEnabled
		 , LastUpdated
		 , LastHandled
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))
END