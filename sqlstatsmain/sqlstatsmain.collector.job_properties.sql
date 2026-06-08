SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [job_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'job_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xDF95EA9A90DC6C49D0A0C634C9E8D10432817352369D6696212BE879BBAD598C

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
	CREATE TABLE [incoming].[job_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[job_id] [uniqueidentifier] NOT NULL,
		[job_name] [nvarchar](128) NOT NULL,
		[description] [nvarchar](512) NOT NULL,
		[job_category] [nvarchar](128) NOT NULL,
		[job_owner] [nvarchar](128) NOT NULL,
		[enabled] [tinyint] NOT NULL,
		[notify_email_desc] [nvarchar](15) NOT NULL,
		[run_status_desc] [nvarchar](15) NOT NULL,
		[last_startdate] [datetime] NOT NULL,
		[last_duration] [decimal](18, 3) NOT NULL,
		[run_duration_avg] [decimal](18, 3) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_job_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[job_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'job_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x6AD2AD32758CF0E67BF88E05D1DFB316FCB475026DBEEAD1772BF63319362152

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
	CREATE TABLE [data].[job_properties](
	    [serverid] [uniqueidentifier] NOT NULL,
		[job_id] [uniqueidentifier] NOT NULL,
		[job_name] [nvarchar](128) NOT NULL,
		[description] [nvarchar](512) NOT NULL,
		[job_category] [nvarchar](128) NOT NULL,
		[job_owner] [nvarchar](128) NOT NULL,
		[enabled] [tinyint] NOT NULL,
		[notify_email_desc] [nvarchar](15) NOT NULL,
		[run_status_desc] [nvarchar](15) NOT NULL,
		[last_startdate] [datetime] NOT NULL,
		[last_duration] [decimal](18, 3) NOT NULL,
		[run_duration_avg] [decimal](18, 3) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_job_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[job_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[job_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[job_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[job_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[job_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-04	Marcus Petö			+Added MERGE function
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[job_properties]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	MERGE [data].[job_properties] dest
	USING
	(
		SELECT
			 [serverid]
			,[job_id]
			,[job_name]
			,[description]
			,[job_category]
			,[job_owner]
			,[enabled]
			,[notify_email_desc]
			,[run_status_desc]
			,[last_startdate]
			,[last_duration]
			,[run_duration_avg]
			,[LastUpdatedUTC]
			,[LastHandledUTC]
		FROM [incoming].[job_properties]
		WHERE	[serverid] = @serverid
	) src
	ON src.[serverid] = dest.[serverid]
	AND src.[job_id] = dest.[job_id]
	WHEN NOT MATCHED THEN
		INSERT 
			(
				 [serverid]
				,[job_id]
				,[job_name]
				,[description]
				,[job_category]
				,[job_owner]
				,[enabled]
				,[notify_email_desc]
				,[run_status_desc]
				,[last_startdate]
				,[last_duration]
				,[run_duration_avg]
				,[LastUpdatedUTC]
				,[LastHandledUTC]
			)
			VALUES
			(
				 src.[serverid]
				,src.[job_id]
				,src.[job_name]
				,src.[description]
				,src.[job_category]
				,src.[job_owner]
				,src.[enabled]
				,src.[notify_email_desc]
				,src.[run_status_desc]
				,src.[last_startdate]
				,src.[last_duration]
				,src.[run_duration_avg]
				,src.[LastUpdatedUTC]
				,src.[LastHandledUTC]
			)
	WHEN MATCHED AND src.[LastUpdatedUTC] <> dest.[LastUpdatedUTC] THEN
		UPDATE SET
					 dest.[serverid] = src.[serverid]
					,dest.[job_id] = src.[job_id]
					,dest.[job_name] = src.[job_name]
					,dest.[description] = src.[description]
					,dest.[job_category] = src.[job_category]
					,dest.[job_owner] = src.[job_owner]
					,dest.[enabled] = src.[enabled]
					,dest.[notify_email_desc] = src.[notify_email_desc]
					,dest.[run_status_desc] = src.[run_status_desc]
					,dest.[last_startdate] = src.[last_startdate]
					,dest.[last_duration] = src.[last_duration]
					,dest.[run_duration_avg] = src.[run_duration_avg]
					,dest.[LastUpdatedUTC] = src.[LastUpdatedUTC]
					,dest.[LastHandledUTC] = src.[LastHandledUTC]
			;

	DELETE FROM [incoming].[job_properties]
	WHERE [serverid] = @serverid
END
GO


RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'agent', 'job_properties', '0 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
