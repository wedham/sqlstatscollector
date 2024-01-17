SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [connection_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'connection_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xEB52791FCA562E9AD9498D7F2C7E8BDCD7C2D6DD25AAD4110AD53ABBADEE732B

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
	CREATE TABLE [data].[connection_properties](
		[db_name] [nvarchar](128) NOT NULL,
		[host_name] [nvarchar](128) NOT NULL,
		[login_name] [nvarchar](128) NOT NULL,
		[program_name] [nvarchar](128) NOT NULL,
		[connection_count] [bigint] NOT NULL,
		[last_seen] [datetime2](0) NOT NULL
	) ON [PRIMARY]
END





RAISERROR(N'/****** Object:  StoredProcedure [collect].[connection_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[connection_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[connection_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[connection_properties]
   -----------------------------------------
   Collects information about applications and users that connect to each database.
   Does NOT catch every single login

Date		Name				Description
----------	-------------		-----------------------------------------------
2023-08-09	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [collect].[connection_properties]
AS
BEGIN
    PRINT('[collect].[connection_properties] - gathering database usage for SQL Server processes')
	SET NOCOUNT ON
	MERGE [data].[connection_properties] dest
	USING (
			SELECT DISTINCT [db_name] = ISNULL( DB_NAME([dbid]), N'')
						  , [host_name] = ISNULL( [hostname], N'')
						  , [login_name] = ISNULL( [loginame], N'')
						  , [program_name] = ISNULL( [program_name], N'')
						  , [last_seen] = SYSUTCDATETIME()
			FROM sys.sysprocesses
			WHERE spid > 50
		 ) src
	ON dest.[db_name] = src.[db_name] COLLATE Finnish_Swedish_CI_AS
		AND dest.[host_name] = src.[host_name] COLLATE Finnish_Swedish_CI_AS
		AND dest.[login_name] = src.[login_name] COLLATE Finnish_Swedish_CI_AS
		AND dest.[program_name] = src.[program_name] COLLATE Finnish_Swedish_CI_AS
	WHEN NOT MATCHED THEN
		INSERT ([db_name], [host_name], [login_name], [program_name], [connection_count], [last_seen])
		VALUES (src.[db_name], src.[host_name], src.[login_name], src.[program_name], 1, src.[last_seen])
	WHEN MATCHED THEN 
		UPDATE SET dest.[last_seen] = src.[last_seen]
		, dest.[connection_count] = dest.[connection_count] + 1
	;
END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'usage', 'connection_properties', '*/20 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01');
GO
