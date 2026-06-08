SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [databasefile_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'databasefile_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xCEC79894EC9386B92B5591C248969CAFAD9C26F335BBC55611258130AFBFFB01

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
	CREATE TABLE [incoming].[databasefile_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtimeutc] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[freespace_mb] [decimal](19, 4) NOT NULL,
		[num_of_reads] [bigint] NOT NULL,
		[num_of_bytes_read] [bigint] NOT NULL,
		[io_stall_read_ms] [bigint] NOT NULL,
		[num_of_writes] [bigint] NOT NULL,
		[num_of_bytes_written] [bigint] NOT NULL,
		[io_stall_write_ms] [bigint] NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_databasefile_stats PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC,
				[database_id] ASC,
				[file_id] ASC
			) ON [PRIMARY]	
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'databasefile_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x0C3CA919BBAEE387DFB234AEB10FC5F8FD6C4C68E323D590054F1FA2288354AE

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
	CREATE TABLE [data].[databasefile_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtimeutc] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[freespace_mb] [decimal](19, 4) NOT NULL,
		[num_of_reads] [bigint] NOT NULL,
		[num_of_bytes_read] [bigint] NOT NULL,
		[io_stall_read_ms] [bigint] NOT NULL,
		[num_of_writes] [bigint] NOT NULL,
		[num_of_bytes_written] [bigint] NOT NULL,
		[io_stall_write_ms] [bigint] NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_databasefile_stats PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC,
				[database_id] ASC,
				[file_id] ASC
			) ON [PRIMARY]	
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[databasefile_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[databasefile_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[databasefile_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[databasefile_stats]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-04	Marcus Petö			+Added INSERT IF NOT EXISTS functionality
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[databasefile_stats]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	INSERT INTO [data].[databasefile_stats]
	(
		 [serverid]
		,[rowtimeutc]
		,[database_id]
		,[file_id]
		,[size_mb]
		,[freespace_mb]
		,[num_of_reads]
		,[num_of_bytes_read]
		,[io_stall_read_ms]
		,[num_of_writes]
		,[num_of_bytes_written]
		,[io_stall_write_ms]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	)
	SELECT
		 [serverid]
		,[rowtimeutc]
		,[database_id]
		,[file_id]
		,[size_mb]
		,[freespace_mb]
		,[num_of_reads]
		,[num_of_bytes_read]
		,[io_stall_read_ms]
		,[num_of_writes]
		,[num_of_bytes_written]
		,[io_stall_write_ms]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	FROM [incoming].[databasefile_stats] src
	WHERE	[serverid] = @serverid 
			AND NOT EXISTS (	
							 SELECT 1 
							 FROM [data].[databasefile_stats] trg 
							 WHERE	src.[serverid] = trg.[serverid]
								AND src.[rowtimeutc] = trg.[rowtimeutc]
								AND src.[database_id] = trg.[database_id]
								AND src.[file_id] = trg.[file_id]
							)
	
	DELETE FROM [incoming].[databasefile_stats]
	WHERE [serverid] = @serverid
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'databasefile_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
