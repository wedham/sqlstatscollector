SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [server_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'server_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xF5F93AC3A64FC8B4834846F94895E02A5A5174D8AF91B08FD9C360C3054D3740

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
	CREATE TABLE [incoming].[server_properties](
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

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
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

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[server_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[server_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[server_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[server_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[server_properties]
AS
BEGIN
	SET NOCOUNT ON
END
GO

