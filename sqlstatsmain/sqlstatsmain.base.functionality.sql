SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'/****** Section:  Schemas ******/', 10, 1) WITH NOWAIT
GO

RAISERROR(N'/****** Object:  Schema [internal] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'internal')) 
BEGIN
    EXEC ('CREATE SCHEMA [internal] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** Object:  Schema [data] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'data')) 
BEGIN
    EXEC ('CREATE SCHEMA [data] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** Object:  Schema [incoming] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'incoming')) 
BEGIN
    EXEC ('CREATE SCHEMA [incoming] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** Object:  Schema [validate] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'validate')) 
BEGIN
    EXEC ('CREATE SCHEMA [validate] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** End Section:  Schemas ******/', 10, 1) WITH NOWAIT
GO

/****** Object:  Table-Value Function  [internal].[TableMetadataChecker] ******/
GO

IF OBJECT_ID(N'[internal].[TableMetadataChecker]', N'IF') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [internal].[TableMetadataChecker] () RETURNS TABLE AS RETURN SELECT x = NULL')
END
GO

/*******************************************************************************
--Copyright (c) 2024 Mikael Wedham (MIT License)
   -----------------------------------------
   [internal].[TableMetadataChecker]
   -----------------------------------------
   Calculates a checksum of the definition of a table.
   The checksum column is returned as a varbinary(32)

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-01-15	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER FUNCTION [internal].[TableMetadataChecker]
(@schemaname nvarchar(128), @tablename nvarchar(128), @tabledefinitionhash varbinary(32))
RETURNS TABLE
AS
RETURN
WITH ListOfTableMetadata AS
    (
        SELECT [schemaname] = @schemaname
		     , [tablename] = @tablename
             , [fullname] = CONCAT('[', schemainfo.[name], '].[', tableinfo.[name], ']')
             , [columndata] = CAST((SELECT columnname = columninfo.[name]
										 , columninfo.[system_type_id]
										 , columninfo.[max_length]
										 , columninfo.[precision]
										 , columninfo.[scale]
										 , columninfo.[is_nullable]
										 , collation_name = ISNULL(columninfo.[collation_name],N'')
									FROM sys.columns columninfo 
									WHERE columninfo.[object_id] = tableinfo.[object_id]
									ORDER BY columnname
									FOR XML AUTO, ROOT(N'columns')) AS XML
								   )
			, [TableExists] = 1
		FROM sys.objects tableinfo 
        INNER JOIN sys.schemas schemainfo 
            ON tableinfo.[schema_id] = schemainfo.[schema_id]
        WHERE tableinfo.[type] = 'U' 
		  AND schemainfo.[name] = @schemaname
		  AND tableinfo.[name] = @tablename
		UNION ALL
        SELECT [schemaname] = @schemaname
		     , [tablename] = @tablename
			 , [fullname] = CAST(CONCAT('[', @schemaname, '].[', @tablename, ']') as nvarchar(256))
			 , [columndata] = CAST(NULL as XML)
			 , [TableExists] = 0
    ), CurrentTableDefinition AS
	(
		SELECT TOP(1) [SchemaName] = [schemaname]
			 , [TableName] = [tablename]
			 , [FullName] = [fullname]
			 , [TableDefinitionHash] = CAST(CASE WHEN [TableExists] = 0 THEN NULL ELSE HASHBYTES('SHA2_256', (SELECT fullname, columndata FROM (VALUES(NULL))keydata(x) FOR JSON AUTO)) END AS varbinary(32))
			 , [TableExists]  = CAST([TableExists] AS int)
		FROM ListOfTableMetadata
		ORDER BY [TableExists] DESC
	)
		SELECT [SchemaName] = [SchemaName]
			 , [TableName] = [TableName]
			 , [FullName] = [FullName]
			 , [TableDefinitionHash] = [TableDefinitionHash]
			 , [TableExists] 
			 , [TableHasChanged] = CAST(CASE WHEN ISNULL(@tabledefinitionhash, 0x00) = [TableDefinitionHash] OR [TableExists] = 0 THEN 0 ELSE 1 END AS int)
		FROM CurrentTableDefinition
GO



RAISERROR(N'/****** Object:  Table [internal].[collectors] ******/', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'internal'
DECLARE @TableName nvarchar(128) = N'collectors'
DECLARE @TableDefinitionHash varbinary(32) = 0x6DFACCC7712EAF9523470730DA67FA0B1F57D50465B6B5D9D1D010A48A5B8F56

DECLARE @TableExists int
DECLARE @TableHasChanged int
DECLARE @FullName nvarchar(255)

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
	RAISERROR(N'Creating [internal].[collectors]', 10, 1) WITH NOWAIT
	CREATE TABLE [internal].[collectors](
		[section] [nvarchar](100) NOT NULL,
		[collector] [nvarchar](100) NOT NULL,
		[cron] [nvarchar](255) NOT NULL,
		[is_enabled] [bit] NOT NULL,
		[lastrun] [datetime2](0) NOT NULL,
		 CONSTRAINT [PK_internal_collectors] PRIMARY KEY CLUSTERED 
			(
				[collector] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO


RAISERROR(N'/****** Object:  Table [internal].[executionlog] ******/', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'internal'
DECLARE @TableName nvarchar(128) = N'executionlog'
DECLARE @TableDefinitionHash varbinary(32) = 0x432F521039C020BE1627537C4600E8CC7E92C2A9F6D04FBAE54E442DED561C49

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
	RAISERROR(N'DROPPING original table', 10, 1) WITH NOWAIT
	SELECT @cmd = N'DROP TABLE ' + @FullName
	EXEC (@cmd)
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	RAISERROR(N'Creating [internal].[executionlog]', 10, 1) WITH NOWAIT
	CREATE TABLE [internal].[executionlog](
		[Id] [bigint] IDENTITY(1,1) NOT NULL,
		[collector] [nvarchar](100) NOT NULL,
		[StartTime] [datetime2](6) NOT NULL,
		[EndTime] [datetime2](6) NULL,
		[Duration_ms] [decimal](18, 3) NULL,
		[errornumber] [int] NULL,
		 CONSTRAINT [PK_internal_executionlog] PRIMARY KEY CLUSTERED 
			(
				[Id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO





RAISERROR(N'/****** Object:  Table [internal].[sqlserverinstances] ******/', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'internal'
DECLARE @TableName nvarchar(128) = N'sqlserverinstances'
DECLARE @TableDefinitionHash varbinary(32) = 0x432F521039C020BE1627537C4600E8CC7E92C2A9F6D04FBAE54E442DED561C49

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
	RAISERROR(N'DROPPING original table', 10, 1) WITH NOWAIT
	SELECT @cmd = N'DROP TABLE ' + @FullName
	EXEC (@cmd)
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	RAISERROR(N'Creating [internal].[sqlserverinstances]', 10, 1) WITH NOWAIT
	CREATE TABLE [internal].[sqlserverinstances] (
		[serverkey] [bigint] IDENTITY (1, 1) NOT NULL,
		[serverid] [uniqueidentifier] NULL,
		[InstanceName] [nvarchar](255) NOT NULL,
		[UserName] [nvarchar](50) NULL,
		[Password] [nvarchar](50) NULL,
		[LastConnection] [datetime2](7) NULL,
		[ConnectionString] [nvarchar](1000) NULL,
		CONSTRAINT [PK_sqlserverinstances] PRIMARY KEY CLUSTERED ([serverkey] ASC)
	);
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

