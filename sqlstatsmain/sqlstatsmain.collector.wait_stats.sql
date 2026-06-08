SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [wait_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'wait_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x10697CE98437712382C992FD2B48674A024653E6E03A77E1D5699CC9B6A71CC2

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
	CREATE TABLE [incoming].[wait_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtimeutc] [datetime2](7) NOT NULL,
		[wait_type] [nvarchar](127) NOT NULL,
		[interval_percentage] [decimal](18, 3) NOT NULL,
		[wait_time_seconds] [decimal](18, 3) NOT NULL,
		[resource_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[signal_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[wait_count] [bigint] NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT [PK_data_wait_stats] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC,
				[wait_type] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'wait_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xBBA75F7A9B3C216B31ED54050C554A5BC8E53772AB369CA18338511494697BCB

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
	CREATE TABLE [data].[wait_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtimeutc] [datetime2](7) NOT NULL,
		[wait_type] [nvarchar](127) NOT NULL,
		[interval_percentage] [decimal](18, 3) NOT NULL,
		[wait_time_seconds] [decimal](18, 3) NOT NULL,
		[resource_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[signal_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[wait_count] [bigint] NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_wait_stats PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC,
				[wait_type] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

RAISERROR(N'/****** Object:  StoredProcedure [transfer].[wait_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[wait_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[wait_stats] AS SELECT NULL')
END
GO

/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[wait_stats]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-04	Marcus Petö			Added INSERT IF NOT EXISTS functionality
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[wait_stats]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	INSERT INTO [data].[wait_stats]
	(
		 [serverid]
		,[rowtimeutc]
		,[wait_type]
		,[interval_percentage]
		,[wait_time_seconds]
		,[resource_wait_time_seconds]
		,[signal_wait_time_seconds]
		,[wait_count]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	)
	SELECT
		 [serverid]
		,[rowtimeutc]
		,[wait_type]
		,[interval_percentage]
		,[wait_time_seconds]
		,[resource_wait_time_seconds]
		,[signal_wait_time_seconds]
		,[wait_count]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	FROM [incoming].[wait_stats] src
	WHERE	[serverid] = @serverid 
			AND NOT EXISTS (	
							 SELECT 1 
							 FROM [data].[wait_stats] trg 
							 WHERE	src.[serverid] = trg.[serverid]
								AND src.[rowtimeutc] = trg.[rowtimeutc]
								AND src.[wait_type] = trg.[wait_type]
							)
	
	DELETE FROM [incoming].[wait_stats]
	WHERE [serverid] = @serverid
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'wait_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
