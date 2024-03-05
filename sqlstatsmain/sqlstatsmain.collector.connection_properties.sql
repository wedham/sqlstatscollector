SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [connection_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'connection_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xB6B28A6F774122795FEB93C5479EF6AF92312FC972267BE54F09C10AD324DBDE

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
	CREATE TABLE [incoming].[connection_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[db_name] [nvarchar](128) NOT NULL,
		[host_name] [nvarchar](128) NOT NULL,
		[login_name] [nvarchar](128) NOT NULL,
		[program_name] [nvarchar](128) NOT NULL,
		[connection_weight] [bigint] NOT NULL,
		[last_seen] [datetime2](0) NOT NULL
	) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'connection_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xFDB110463139C2B7C528463DC9CDFB6468441298C469A401D0AA73EC9487EEF3

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
	CREATE TABLE [data].[connection_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[db_name] [nvarchar](128) NOT NULL,
		[host_name] [nvarchar](128) NOT NULL,
		[login_name] [nvarchar](128) NOT NULL,
		[program_name] [nvarchar](128) NOT NULL,
		[connection_weight] [bigint] NOT NULL,
		[last_seen] [datetime2](0) NOT NULL
	) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[connection_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[connection_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[connection_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[connection_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[connection_properties]
AS
BEGIN
	SET NOCOUNT ON
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'usage', 'connection_properties', '*/20 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 0);
GO
