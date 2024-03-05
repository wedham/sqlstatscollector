SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [memory_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'memory_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xF516402DE84062D10A8822CB35A94EB5083E26F65674C2EA09C39412C6E12E85

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
	CREATE TABLE [incoming].[memory_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtime] [datetime2](7) NOT NULL,
		[page_life_expectancy] [int] NOT NULL,
		[target_server_memory_mb] [bigint] NOT NULL,
		[total_server_memory_mb] [bigint] NOT NULL,
		[total_physical_memory_mb] [bigint] NOT NULL,
		[available_physical_memory_mb] [bigint] NOT NULL,
		[percent_memory_used] [decimal](18, 3) NOT NULL,
		[system_memory_state_desc] [nvarchar](256) NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_memory_stats PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtime] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'memory_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xFB5F8A49949F92007A57717249BB3D25160337E4AB7C04EE8F6147EA858392C0

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
	CREATE TABLE [data].[memory_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtime] [datetime2](7) NOT NULL,
		[page_life_expectancy] [int] NOT NULL,
		[target_server_memory_mb] [bigint] NOT NULL,
		[total_server_memory_mb] [bigint] NOT NULL,
		[total_physical_memory_mb] [bigint] NOT NULL,
		[available_physical_memory_mb] [bigint] NOT NULL,
		[percent_memory_used] [decimal](18, 3) NOT NULL,
		[system_memory_state_desc] [nvarchar](256) NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_memory_stats PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtime] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[memory_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[memory_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[memory_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[memory_stats]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[memory_stats]
AS
BEGIN
	SET NOCOUNT ON
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'memory_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
