SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


RAISERROR(N'Collector: [databasefile_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'databasefile_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x3CE63596CF0A16CFDF4B3344FEAEB8D67392124B4BD1566213BDEC952BD72CAC

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
	CREATE TABLE [incoming].[databasefile_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[type_desc] [nvarchar](60) NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[physical_name] [nvarchar](260) NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[max_size_mb] [int] NULL,
		[growth_mb] [int] NULL,
		[growth_percent] [int] NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_databasefile_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[database_id] ASC,
				[file_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'databasefile_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x3CE63596CF0A16CFDF4B3344FEAEB8D67392124B4BD1566213BDEC952BD72CAC

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
	CREATE TABLE [data].[databasefile_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[type_desc] [nvarchar](60) NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[physical_name] [nvarchar](260) NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[max_size_mb] [int] NULL,
		[growth_mb] [int] NULL,
		[growth_percent] [int] NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_databasefile_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[database_id] ASC,
				[file_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[databasefile_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[databasefile_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[databasefile_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[databasefile_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[databasefile_properties]
AS
BEGIN
	SET NOCOUNT ON
END
GO
