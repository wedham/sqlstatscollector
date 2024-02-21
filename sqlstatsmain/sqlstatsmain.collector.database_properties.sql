SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [database_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'internal'
DECLARE @TableName nvarchar(128) = N'database_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x96D72BB444325A99A1D141BD897626C768056A79E7CC26157130409B2AB55D24

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
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_database_properties] PRIMARY KEY CLUSTERED 
			(
				[database_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

RAISERROR(N'/****** Object:  StoredProcedure [transfer].[database_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[database_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[database_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[database_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_properties]
AS
BEGIN
	SET NOCOUNT ON
END
GO

