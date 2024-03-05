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
DECLARE @TableDefinitionHash varbinary(32) = 0x64DEF7CC6E8BC9724A2033A815802A5233E17E80512C1A70D60172F0554B5C71

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
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_job_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[job_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'job_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xD8B069FB9D800DF432952FD6A96E5A1E556915B628F93CA3E9C2103D6296D336

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
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_job_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC,
				[job_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
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
*******************************************************************************/
ALTER PROCEDURE [transfer].[job_properties]
AS
BEGIN
	SET NOCOUNT ON
END
GO

