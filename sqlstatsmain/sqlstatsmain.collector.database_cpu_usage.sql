SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [database_cpu_usage]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'database_cpu_usage'
DECLARE @TableDefinitionHash varbinary(32) = 0x84E03A541EA3EA69608469565900DE884DD69BA5C3B1267352B7AF75AFF45000

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
	CREATE TABLE [incoming].[database_cpu_usage](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtime] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[cpu_time_ms] [decimal](18, 3) NOT NULL,
		[cpu_percent] [decimal](5, 2) NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_database_cpu_usage PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtime] ASC,
				[database_id] ASC
			) ON [PRIMARY]
	) ON [PRIMARY]

END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'database_cpu_usage'
DECLARE @TableDefinitionHash varbinary(32) = 0x058AEC8E37D8AFDF72CF2CED63C2B1C2118B6E4BE37F4331383B4EE06EA9587E

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
	CREATE TABLE [data].[database_cpu_usage](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtime] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[cpu_time_ms] [decimal](18, 3) NOT NULL,
		[cpu_percent] [decimal](5, 2) NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_database_cpu_usage PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtime] ASC,
				[database_id] ASC
			) ON [PRIMARY]
	) ON [PRIMARY]

END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[database_cpu_usage] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[database_cpu_usage]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[database_cpu_usage] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_cpu_usage]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_cpu_usage]
AS
BEGIN
	SET NOCOUNT ON
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'database_cpu_usage', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
