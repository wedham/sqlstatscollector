SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [server_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'server_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xFEC8ECA4B3383EF19EDA4C43C0B84B94D7EAC39192AA97019B94F602F7D2C6DD

DECLARE @TableExists int
DECLARE @TableHasChanged int
DECLARE @FullName nvarchar(255)
DECLARE @NewName nvarchar(128)

DECLARE @cmd nvarchar(2048)
DECLARE @msg nvarchar(2048)

SELECT @FullName = [FullName]
     , @TableExists = [TableExists]
     , @TableHasChanged = [TableHasChanged]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

IF @TableExists = 1 AND @TableHasChanged = 1
BEGIN
	RAISERROR(N'DROP original table', 10, 1) WITH NOWAIT
	SELECT @cmd = N'DROP TABLE ' + @FullName
	EXEC (@cmd)
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	SELECT @msg = N'Creating ' + @FullName
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	CREATE TABLE [data].[server_properties](
		[serverid] [uniqueidentifier] NOT NULL,
		[MachineName] [nvarchar](128) NOT NULL,
		[ServerName] [nvarchar](128) NOT NULL,
		[Instance] [nvarchar](128) NULL,
		[ComputerNamePhysicalNetBIOS] [nvarchar](128) NULL,
		[Edition] [nvarchar](128) NOT NULL,
		[ProductLevel] [nvarchar](128) NOT NULL,
		[ProductVersion] [nvarchar](128) NOT NULL,
		[Collation] [nvarchar](128) NOT NULL,
		[IsClustered] [int] NULL,
		[IsIntegratedSecurityOnly] [int] NULL,
		[FilestreamConfiguredLevel] [int] NULL,
		[IsHadrEnabled] [int] NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_data_server_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END
GO



RAISERROR(N'/****** Object:  StoredProcedure [collect].[server_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[server_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[server_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[server_properties]
   -----------------------------------------
   Collects information about the server/instance.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2024-01-17  Mikael Wedham		+Added value generation for serverid
*******************************************************************************/
ALTER PROCEDURE [collect].[server_properties]
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
		INSERT ([serverid]
		       ,[MachineName]
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
			   (NEWID()
			   ,[MachineName]
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
		   [ServerName] = src.[ServerName]
		  ,[Instance] = src.[Instance]
		  ,[ComputerNamePhysicalNetBIOS] = src.[ComputerNamePhysicalNetBIOS]
		  ,[Edition] = src.[Edition]
		  ,[ProductLevel] = src.[ProductLevel]
		  ,[ProductVersion] = src.[ProductVersion]
		  ,[Collation] = src.[Collation]
		  ,[IsClustered] = src.[IsClustered]
		  ,[IsIntegratedSecurityOnly] = src.[IsIntegratedSecurityOnly]
		  ,[FilestreamConfiguredLevel] = src.[FilestreamConfiguredLevel]
		  ,[IsHadrEnabled] = src.[IsHadrEnabled]
		  ,[LastUpdated] = SYSUTCDATETIME();
END
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[server_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[server_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[server_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[server_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[server_properties]
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
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'server_properties', '0 6 * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01');
GO
