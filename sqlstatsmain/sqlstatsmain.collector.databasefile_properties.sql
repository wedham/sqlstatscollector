SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


RAISERROR(N'Collector: [databasefile_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'databasefile_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x011A91B2A9CB71D61B6BB75D680F5C0D946E182E112EAF71359C584D514C36CC

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
	CREATE TABLE [incoming].[databasefile_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[type_desc] [nvarchar](60) NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[physical_name] [nvarchar](260) NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[max_size_mb] [int] NULL,
		[growth_mb] [int] NULL,
		[growth_percent] [int] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_databasefile_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
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
DECLARE @TableName nvarchar(128) = N'databasefile_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x3D36A148ED691536D9766B14F2103F75257FA3AF70C7C2FB62B1B4BB9361E56A

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
	CREATE TABLE [data].[databasefile_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[type_desc] [nvarchar](60) NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[physical_name] [nvarchar](260) NOT NULL,
		[state_desc] [nvarchar](60) NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[max_size_mb] [int] NULL,
		[growth_mb] [int] NULL,
		[growth_percent] [int] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_databasefile_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[database_id] ASC,
				[file_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[databasefile_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[databasefile_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[databasefile_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[databasefile_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-04	Marcus Petö			+Added MERGE function
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[databasefile_properties]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	MERGE [data].[databasefile_properties] dest
	USING
	(
		SELECT
			 [serverid]
			,[database_id]
			,[file_id]
			,[type_desc]
			,[name]
			,[physical_name]
			,[state_desc]
			,[size_mb]
			,[max_size_mb]
			,[growth_mb]
			,[growth_percent]
			,[LastUpdatedUTC]
			,[LastHandledUTC]
		FROM [incoming].[databasefile_properties]
		WHERE	[serverid] = @serverid
	) src
	ON src.[serverid] = dest.[serverid]
	AND src.[database_id] = dest.[database_id]
	AND src.[file_id] = dest.[file_id]
	WHEN NOT MATCHED THEN
		INSERT 
			(
				 [serverid]
				,[database_id]
				,[file_id]
				,[type_desc]
				,[name]
				,[physical_name]
				,[state_desc]
				,[size_mb]
				,[max_size_mb]
				,[growth_mb]
				,[growth_percent]
				,[LastUpdatedUTC]
				,[LastHandledUTC]
			)
			VALUES
			(
				 src.[serverid]
				,src.[database_id]
				,src.[file_id]
				,src.[type_desc]
				,src.[name]
				,src.[physical_name]
				,src.[state_desc]
				,src.[size_mb]
				,src.[max_size_mb]
				,src.[growth_mb]
				,src.[growth_percent]
				,src.[LastUpdatedUTC]
				,src.[LastHandledUTC]
			)
	WHEN MATCHED AND src.[LastUpdatedUTC] <> dest.[LastUpdatedUTC] THEN
		UPDATE SET
					 dest.[serverid] = src.[serverid]
					,dest.[database_id] = src.[database_id]
					,dest.[file_id] = src.[file_id]
					,dest.[type_desc] = src.[type_desc]
					,dest.[name] = src.[name]
					,dest.[physical_name] = src.[physical_name]
					,dest.[state_desc] = src.[state_desc]
					,dest.[size_mb] = src.[size_mb]
					,dest.[max_size_mb] = src.[max_size_mb]
					,dest.[growth_mb] = src.[growth_mb]
					,dest.[growth_percent] = src.[growth_percent]
					,dest.[LastUpdatedUTC] = src.[LastUpdatedUTC]
					,dest.[LastHandledUTC] = src.[LastHandledUTC]
			;

	DELETE FROM [incoming].[databasefile_properties]
	WHERE [serverid] = @serverid
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'databasefile_properties', '0 */6 * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
