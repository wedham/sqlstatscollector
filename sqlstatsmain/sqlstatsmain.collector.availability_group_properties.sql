SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [availability_group_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'availability_group_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x38CFF59F5A74EDF914E0DF58C5CDC433367723DD5C0B2EE3BBD0FFD2FF8402BA

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
	CREATE TABLE [incoming].[availability_group_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[group_id] [uniqueidentifier] NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[primary_replica] [nvarchar](128) NOT NULL,
		[recovery_health_desc] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'availability_group_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x2F2B47122309C1B6E0FE935A723500724AA4E8559CD9BEE88A93253E4D3D9F71

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
	CREATE TABLE [data].[availability_group_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[group_id] [uniqueidentifier] NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[primary_replica] [nvarchar](128) NOT NULL,
		[recovery_health_desc] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_availability_group_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[group_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[availability_group_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[availability_group_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[availability_group_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[availability_group_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-03	Marcus Petö			+Added MERGE function
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[availability_group_properties]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	MERGE [data].[availability_group_properties] dest
	USING
	(
		SELECT
			 [serverid]
			,[group_id]
			,[name]
			,[primary_replica]
			,[recovery_health_desc]
			,[synchronization_health_desc]
			,[LastUpdatedUTC]
			,[LastHandledUTC]
		FROM [incoming].[availability_group_properties]
		WHERE	[serverid] = @serverid
	) src
	ON src.[serverid] = dest.[serverid]
	AND src.[group_id] = dest.[group_id]
	WHEN NOT MATCHED THEN
		INSERT 
			(
				 [serverid]
				,[group_id]
				,[name]
				,[primary_replica]
				,[recovery_health_desc]
				,[synchronization_health_desc]
				,[LastUpdatedUTC]
				,[LastHandledUTC]
			)
			VALUES
			(
				 src.[serverid]
				,src.[group_id]
				,src.[name]
				,src.[primary_replica]
				,src.[recovery_health_desc]
				,src.[synchronization_health_desc]
				,src.[LastUpdatedUTC]
				,src.[LastHandledUTC]
			)
	WHEN MATCHED AND src.[LastUpdatedUTC] <> dest.[LastUpdatedUTC] THEN
		UPDATE SET
				 dest.[serverid] = src.[serverid]
				,dest.[group_id] = src.[group_id]
				,dest.[name] = src.[name]
				,dest.[primary_replica] = src.[primary_replica]
				,dest.[recovery_health_desc] =  src.[recovery_health_desc]
				,dest.[synchronization_health_desc] = src.[synchronization_health_desc]
				,dest.[LastUpdatedUTC] = src.[LastUpdatedUTC]
				,dest.[LastHandledUTC] = src.[LastHandledUTC]
			;

	DELETE FROM [incoming].[availability_group_properties]
	WHERE [serverid] = @serverid
END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'hadr', 'availability_group_properties', '*/20 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
