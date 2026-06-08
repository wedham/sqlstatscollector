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
DECLARE @TableDefinitionHash varbinary(32) = 0xDC1C7E95D31F3FC26F9C62413D7D3410B55A264200207623882DC932D14988AC

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
		[rowtimeutc] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[cpu_time_ms] [decimal](18, 3) NOT NULL,
		[cpu_percent] [decimal](5, 2) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_database_cpu_usage PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC,
				[database_id] ASC
			) ON [PRIMARY]
	) ON [PRIMARY]

END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'database_cpu_usage'
DECLARE @TableDefinitionHash varbinary(32) = 0xD7F3FF7BCA11B9BDB0FA984E238F09BF8CCAE568B94645EE82BA72F6A0B793E5

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
		[rowtimeutc] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[cpu_time_ms] [decimal](18, 3) NOT NULL,
		[cpu_percent] [decimal](5, 2) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_database_cpu_usage PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC,
				[database_id] ASC
			) ON [PRIMARY]
	) ON [PRIMARY]

END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
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
2026-02-04	Marcus Petö			+Added INSERT IF NOT EXISTS functionality
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_cpu_usage]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	INSERT INTO [data].[database_cpu_usage]
	(
		 [serverid]
		,[rowtimeutc]
		,[database_id]
		,[cpu_time_ms]
		,[cpu_percent]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	)
	SELECT
		 [serverid]
		,[rowtimeutc]
		,[database_id]
		,[cpu_time_ms]
		,[cpu_percent]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	FROM [incoming].[database_cpu_usage] src
	WHERE	[serverid] = @serverid 
			AND NOT EXISTS (	
							 SELECT 1 
							 FROM [data].[database_cpu_usage] trg 
							 WHERE	src.[serverid] = trg.[serverid]
								AND src.[rowtimeutc] = trg.[rowtimeutc]
								AND src.[database_id] = trg.[database_id]
							)
	
	DELETE FROM [incoming].[database_cpu_usage]
	WHERE [serverid] = @serverid
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
