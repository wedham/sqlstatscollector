SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [cpu_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'cpu_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xBF80863C03B501C01AFB166FC487C7B490096AF8A24CEDC193C86BEEBB62140F

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
	CREATE TABLE [incoming].[cpu_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtime] [datetime2](3) NOT NULL,
		[rowdate] [date] NOT NULL,
		[record_id] [int] NOT NULL,
		[idle_cpu] [int] NOT NULL,
		[sql_cpu] [int] NOT NULL,
		[other_cpu] [int] NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_cpu_stats] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowdate] ASC,
				[record_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'cpu_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xB7F1FCD687797587F695A7EC77494268859CF61915F342EAE1317B9B707C19E4

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
	CREATE TABLE [data].[cpu_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtime] [datetime2](3) NOT NULL,
		[rowdate] [date] NOT NULL,
		[record_id] [int] NOT NULL,
		[idle_cpu] [int] NOT NULL,
		[sql_cpu] [int] NOT NULL,
		[other_cpu] [int] NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_cpu_stats] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowdate] ASC,
				[record_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[cpu_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[cpu_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[cpu_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[cpu_stats]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[cpu_stats]
AS
BEGIN
	SET NOCOUNT ON
END
GO

