SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [server_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'server_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xB9C57CFFF9890D051583294504DECBFFB320FACAC3238E9E8B4919F9FDCF7586

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
	CREATE TABLE [incoming].[server_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtimeutc] [datetime2](7) NOT NULL,
		[user_connections] [int] NOT NULL,
		[batch_requests_sec] [int] NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_server_stats PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'server_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x4EB0C8F630A154FB97CFA50EBD220142B0039DFD5B51F12B8C084D85E42F9D88

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
	CREATE TABLE [data].[server_stats](
	    [serverid] [uniqueidentifier] NOT NULL,
		[rowtimeutc] [datetime2](7) NOT NULL,
		[user_connections] [int] NOT NULL,
		[batch_requests_sec] [int] NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_server_stats PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[rowtimeutc] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[server_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[server_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[server_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[server_stats]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-04	Marcus Petö			+Added INSERT IF NOT EXISTS functionality
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[server_stats]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	INSERT INTO [data].[server_stats]
	(
		 [serverid]
		,[rowtimeutc]
		,[user_connections]
		,[batch_requests_sec]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	)
	SELECT
		 [serverid]
		,[rowtimeutc]
		,[user_connections]
		,[batch_requests_sec]
		,[LastUpdatedUTC]
		,[LastHandledUTC]
	FROM [incoming].[server_stats] src
	WHERE	[serverid] = @serverid 
			AND NOT EXISTS (	
							 SELECT 1 
							 FROM [data].[server_stats] trg 
							 WHERE	src.[serverid] = trg.[serverid]
								AND src.[rowtimeutc] = trg.[rowtimeutc]
							)
	
	DELETE FROM [incoming].[server_stats]
	WHERE [serverid] = @serverid
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'server_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
