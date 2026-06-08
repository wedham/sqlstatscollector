SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [database_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'database_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x0AD8ACA1C8033B4D7B1F7337F5ABF213A394B56EBE0B0236E1BEA3A6E5DFDC50

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
	CREATE TABLE [incoming].[database_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[owner_sid] [varbinary](85) NOT NULL,
		[create_date] [datetime] NOT NULL,
		[compatibility_level] [tinyint] NOT NULL,
		[collation_name] [nvarchar](128) NULL,
		[is_auto_close_on] [bit] NOT NULL,
		[is_auto_shrink_on] [bit] NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[recovery_model_desc] [nvarchar](60) NOT NULL,
		[page_verify_option_desc] [nvarchar](60) NOT NULL,
		[LastFullBackupTime] [datetime] NULL,
		[LastDiffBackupTime] [datetime] NULL,
		[LastLogBackupTime] [datetime] NULL,
		[LastKnownGoodDBCCTime] [datetime] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_database_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[database_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'database_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x4C777F72AC12A851EFA72BBD39A7D20D7BE24A319C01FB6BCE0939BD250052EB

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
	CREATE TABLE [data].[database_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[owner_sid] [varbinary](85) NOT NULL,
		[create_date] [datetime] NOT NULL,
		[compatibility_level] [tinyint] NOT NULL,
		[collation_name] [nvarchar](128) NULL,
		[is_auto_close_on] [bit] NOT NULL,
		[is_auto_shrink_on] [bit] NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[recovery_model_desc] [nvarchar](60) NOT NULL,
		[page_verify_option_desc] [nvarchar](60) NOT NULL,
		[LastFullBackupTime] [datetime] NULL,
		[LastDiffBackupTime] [datetime] NULL,
		[LastLogBackupTime] [datetime] NULL,
		[LastKnownGoodDBCCTime] [datetime] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_database_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[database_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[database_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[database_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[database_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-04	Marcus Petö			+Added MERGE function
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_properties]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	MERGE [data].[database_properties] dest
	USING
	(
		SELECT
			 [serverid]
			,[database_id]
			,[name]
			,[owner_sid]
			,[create_date]
			,[compatibility_level]
			,[collation_name]
			,[is_auto_close_on]
			,[is_auto_shrink_on]
			,[state_desc]
			,[recovery_model_desc]
			,[page_verify_option_desc]
			,[LastFullBackupTime]
			,[LastDiffBackupTime]
			,[LastLogBackupTime]
			,[LastKnownGoodDBCCTime]
			,[LastUpdatedUTC]
			,[LastHandledUTC]
		FROM [incoming].[database_properties]
		WHERE	[serverid] = @serverid
	) src
	ON src.[serverid] = dest.[serverid]
	AND src.[database_id] = dest.[database_id]
	WHEN NOT MATCHED THEN
		INSERT 
			(
				 [serverid]
				,[database_id]
				,[name]
				,[owner_sid]
				,[create_date]
				,[compatibility_level]
				,[collation_name]
				,[is_auto_close_on]
				,[is_auto_shrink_on]
				,[state_desc]
				,[recovery_model_desc]
				,[page_verify_option_desc]
				,[LastFullBackupTime]
				,[LastDiffBackupTime]
				,[LastLogBackupTime]
				,[LastKnownGoodDBCCTime]
				,[LastUpdatedUTC]
				,[LastHandledUTC]
			)
			VALUES
			(
				 src.[serverid]
				,src.[database_id]
				,src.[name]
				,src.[owner_sid]
				,src.[create_date]
				,src.[compatibility_level]
				,src.[collation_name]
				,src.[is_auto_close_on]
				,src.[is_auto_shrink_on]
				,src.[state_desc]
				,src.[recovery_model_desc]
				,src.[page_verify_option_desc]
				,src.[LastFullBackupTime]
				,src.[LastDiffBackupTime]
				,src.[LastLogBackupTime]
				,src.[LastKnownGoodDBCCTime]
				,src.[LastUpdatedUTC]
				,src.[LastHandledUTC]
			)
	WHEN MATCHED AND src.[LastUpdatedUTC] <> dest.[LastUpdatedUTC] THEN
		UPDATE SET
					 dest.[serverid] = src.[serverid]
					,dest.[database_id] = src.[database_id]
					,dest.[name] = src.[name]
					,dest.[owner_sid] = src.[owner_sid]
					,dest.[create_date] = src.[create_date]
					,dest.[compatibility_level] = src.[compatibility_level]
					,dest.[collation_name] = src.[collation_name]
					,dest.[is_auto_close_on] = src.[is_auto_close_on]
					,dest.[is_auto_shrink_on] = src.[is_auto_shrink_on]
					,dest.[state_desc] = src.[state_desc]
					,dest.[recovery_model_desc] = src.[recovery_model_desc]
					,dest.[page_verify_option_desc] = src.[page_verify_option_desc]
					,dest.[LastFullBackupTime] = src.[LastFullBackupTime]
					,dest.[LastDiffBackupTime] = src.[LastDiffBackupTime]
					,dest.[LastLogBackupTime] = src.[LastLogBackupTime]
					,dest.[LastKnownGoodDBCCTime] = src.[LastKnownGoodDBCCTime]
					,dest.[LastUpdatedUTC] = src.[LastUpdatedUTC]
					,dest.[LastHandledUTC] = src.[LastHandledUTC]
			;

	DELETE FROM [incoming].[database_properties]
	WHERE [serverid] = @serverid
END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'database_properties', '0 6 * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
