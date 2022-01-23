
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[server_properties]
   -----------------------------------------
   Collects information about the server/instance.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [collect].[server_properties]
AS
BEGIN
PRINT('[collect].[server_properties] - Get all properties of the current server//instance')
SET NOCOUNT ON

	DECLARE @FilestreamConfiguredLevel int = null
	DECLARE @IsHadrEnabled int = null

	DECLARE @v varchar(20)

	SELECT @v = [internal].[GetSQLServerVersion]()

	IF (@v NOT IN ('2005', '2008', '2008R2'))
	BEGIN
	  SELECT @FilestreamConfiguredLevel = CAST(SERVERPROPERTY('FilestreamConfiguredLevel') AS int)
		   , @IsHadrEnabled = CAST(SERVERPROPERTY('IsHadrEnabled') AS int)
	END;

	MERGE  [data].[server_properties] dest
	USING ( SELECT [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))
				 , [ServerName] = CAST(SERVERPROPERTY('ServerName') AS nvarchar(128))
				 , [Instance] = CAST(SERVERPROPERTY('InstanceName') AS nvarchar(128))
				 , [ComputerNamePhysicalNetBIOS] = CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS nvarchar(128))
				 , [Edition] = CAST(SERVERPROPERTY('Edition') AS nvarchar(128))
				 , [ProductLevel] = CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128))
				 , [ProductVersion] = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))
				 , [Collation] = CAST(SERVERPROPERTY('Collation') AS nvarchar(128))
				 , [IsClustered] = CAST(SERVERPROPERTY('IsClustered') AS int)
				 , [IsIntegratedSecurityOnly] = CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS int)
				 , [FilestreamConfiguredLevel] = @FilestreamConfiguredLevel
				 , [IsHadrEnabled] = @IsHadrEnabled) src
	ON dest.[MachineName] = src.[MachineName]	
	WHEN NOT MATCHED THEN
		INSERT ([MachineName]
			   ,[ServerName]
			   ,[Instance]
			   ,[ComputerNamePhysicalNetBIOS]
			   ,[Edition]
			   ,[ProductLevel]
			   ,[ProductVersion]
			   ,[Collation]
			   ,[IsClustered]
			   ,[IsIntegratedSecurityOnly]
			   ,[FilestreamConfiguredLevel]
			   ,[IsHadrEnabled]
			   ,[LastUpdated] )
		 VALUES
			   ([MachineName]
			   ,[ServerName]
			   ,[Instance]
			   ,[ComputerNamePhysicalNetBIOS]
			   ,[Edition]
			   ,[ProductLevel]
			   ,[ProductVersion]
			   ,[Collation]
			   ,[IsClustered]
			   ,[IsIntegratedSecurityOnly]
			   ,[FilestreamConfiguredLevel]
			   ,[IsHadrEnabled]
			   ,SYSUTCDATETIME())
	WHEN MATCHED THEN
		UPDATE SET 
		   [ServerName] = src.ServerName
		  ,[Instance] = src.Instance
		  ,[ComputerNamePhysicalNetBIOS] = src.ComputerNamePhysicalNetBIOS
		  ,[Edition] = src.Edition
		  ,[ProductLevel] = src.ProductLevel
		  ,[ProductVersion] = src.ProductVersion
		  ,[Collation] = src.Collation
		  ,[IsClustered] = src.IsClustered
		  ,[IsIntegratedSecurityOnly] = src.IsIntegratedSecurityOnly
		  ,[FilestreamConfiguredLevel] = src.FilestreamConfiguredLevel
		  ,[IsHadrEnabled] = src.IsHadrEnabled
		  ,[LastUpdated] = SYSUTCDATETIME();
END
