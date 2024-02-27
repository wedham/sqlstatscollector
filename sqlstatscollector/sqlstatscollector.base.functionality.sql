SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'/****** Section:  Schemas ******/', 10, 1) WITH NOWAIT
GO

RAISERROR(N'/****** Object:  Schema [collect] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'collect')) 
BEGIN
    EXEC ('CREATE SCHEMA [collect] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** Object:  Schema [data] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'data')) 
BEGIN
    EXEC ('CREATE SCHEMA [data] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** Object:  Schema [internal_data] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'internal_data')) 
BEGIN
    EXEC ('CREATE SCHEMA [internal_data] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** Object:  Schema [internal] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'internal')) 
BEGIN
    EXEC ('CREATE SCHEMA [internal] AUTHORIZATION [dbo]')
END
GO

RAISERROR(N'/****** Object:  Schema [transfer] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transfer')) 
BEGIN
    EXEC ('CREATE SCHEMA [transfer] AUTHORIZATION [dbo]')
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

RAISERROR(N'/****** Section:  Synonyms ******/', 10, 1) WITH NOWAIT
GO

RAISERROR(N'/****** Object:  Synonym [dbo].[backupset] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = 'backupset')) 
BEGIN
    EXEC ('CREATE SYNONYM [dbo].[backupset] FOR [msdb].[dbo].[backupset]')
END
GO

RAISERROR(N'/****** Object:  Synonym [dbo].[sysjobs] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = 'sysjobs')) 
BEGIN
    EXEC ('CREATE SYNONYM [dbo].[sysjobs] FOR [msdb].[dbo].[sysjobs]')
END
GO

RAISERROR(N'/****** Object:  Synonym [dbo].[sysjobhistory] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = 'sysjobhistory')) 
BEGIN
    EXEC ('CREATE SYNONYM [dbo].[sysjobhistory] FOR [msdb].[dbo].[sysjobhistory]')
END
GO

RAISERROR(N'/****** Object:  Synonym [dbo].[syscategories] ******/', 10, 1) WITH NOWAIT
IF (NOT EXISTS (SELECT * FROM sys.synonyms WHERE name = 'syscategories')) 
BEGIN
    EXEC ('CREATE SYNONYM [dbo].[syscategories] FOR [msdb].[dbo].[syscategories]')
END
GO

RAISERROR(N'/****** End Section:  Synonyms ******/', 10, 1) WITH NOWAIT
GO



RAISERROR(N'/****** Object:  UserDefinedFunction [internal].[GetSQLServerVersion] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[internal].[GetSQLServerVersion]', N'FN') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [internal].[GetSQLServerVersion] () RETURNS varchar(20) AS BEGIN RETURN NULL END')
END
GO

ALTER FUNCTION [internal].[GetSQLServerVersion]()
RETURNS varchar(20)
WITH SCHEMABINDING
AS
BEGIN

  DECLARE @result varchar(20)

  DECLARE @version varchar(128)

  SELECT @version = CONVERT(varchar(128), SERVERPROPERTY('ProductVersion'))

  SELECT @result =
         CASE WHEN @version LIKE '9%'     THEN '2005'
              WHEN @version LIKE '10%'    THEN '2008'
              WHEN @version LIKE '10.5%'  THEN '2008R2'
              WHEN @version LIKE '11%'    THEN '2012'
              WHEN @version LIKE '12%'    THEN '2014'
              WHEN @version LIKE '13%'    THEN '2016'
              WHEN @version LIKE '14%'    THEN '2017'
              WHEN @version LIKE '15%'    THEN '2019'
              WHEN @version LIKE '16%'    THEN '2022'
   ELSE 'Unknown' END

  RETURN (@result)

END
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
             , [fullname] = '[' + schemainfo.[name] + '].[' + tableinfo.[name] + ']'
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
			 , [fullname] = CAST('[' + @schemaname + '].[' + @tablename + ']' as nvarchar(256))
			 , [columndata] = CAST(NULL as XML)
			 , [TableExists] = 0
    ), CurrentTableDefinition AS
	(
		SELECT TOP(1) [SchemaName] = [schemaname]
			 , [TableName] = [tablename]
			 , [FullName] = [fullname]
			 , [TableDefinitionHash] = CAST(CASE WHEN [TableExists] = 0 THEN NULL ELSE HASHBYTES('SHA2_256', (SELECT fullname, columndata FROM (VALUES(NULL))keydata(x) FOR XML AUTO)) END AS varbinary(32))
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
DECLARE @TableDefinitionHash varbinary(32) = 0x667F123B3437DD7D40137F3892380EA62BBD8A66B0D588EE20BFED3DE6546754

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

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

RAISERROR(N'/****** Object:  Table [internal].[executionlog] ******/', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'internal'
DECLARE @TableName nvarchar(128) = N'executionlog'
DECLARE @TableDefinitionHash varbinary(32) = 0x791191304DF49CD6686A355781A07B272603EF2A04F38F17F97DFEE5607F467E

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

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

RAISERROR(N'/****** Object:  StoredProcedure [internal].[collectors_for_transfer] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[internal].[collectors_for_transfer]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [internal].[collectors_for_transfer] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [internal].[collectors_for_transfer]
   -----------------------------------------
   Returns collector names. Used for transferring data to central database

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [internal].[collectors_for_transfer]
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT [collector] = '[' + [collector] + ']'
	     , [serverid] = @serverid
    FROM [internal].[collectors]
	--WHERE [collector] NOT IN ('') --Filter on collectors that shouldn't be transferred
END
GO

