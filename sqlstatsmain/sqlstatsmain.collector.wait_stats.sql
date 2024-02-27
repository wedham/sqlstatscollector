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
DECLARE @TableDefinitionHash varbinary(32) = 0x9FE9DD22A888D63238D910BB3B2B7D25C0BA8E3D3DBE2BC312AD55C2A2440414

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
		[rowtime] [datetime2](7) NOT NULL,
		[wait_type] [nvarchar](127) NOT NULL,
		[interval_percentage] [decimal](18, 3) NOT NULL,
		[wait_time_seconds] [decimal](18, 3) NOT NULL,
		[resource_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[signal_wait_time_seconds] [decimal](18, 3) NOT NULL,
		[wait_count] [bigint] NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_wait_stats PRIMARY KEY CLUSTERED 
			(
				rowtime ASC,
				wait_type ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT [FullName], [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[wait_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[wait_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[wait_stats] AS SELECT NULL')
END
GO

/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[wait_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[wait_stats]
AS
BEGIN
	SET NOCOUNT ON
END
GO
