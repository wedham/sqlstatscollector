SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [database_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
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
GO




RAISERROR(N'/****** Object:  StoredProcedure [collect].[database_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[database_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[database_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[database_properties]
   -----------------------------------------
   Collects information about the databases.
   This information is kept as one row per database
   Rows are updated/changed and there will be no history of changes

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2022-05-04	Mikael Wedham		+Try/Catch for DBCC where database 
                                 is unavailable due to AOAG
*******************************************************************************/
ALTER PROCEDURE [collect].[database_properties]
AS
BEGIN
PRINT('[collect].[database_properties] - Collection of metadata of databases.')

SET NOCOUNT ON
/*  */
DECLARE @temp TABLE ([ParentObject] varchar(255)
                   , [Object] varchar(255)
				   , [Field] varchar(255)
				   , [Value] varchar(255) )

/*  */
DECLARE @dbccresults TABLE ([database_name] sysname 
                          , [dbccLastKnownGood] datetime
						  , [RowNum] int)

DECLARE	@dbname sysname
DECLARE @SQL varchar(512);

DECLARE @databases TABLE ([name] nvarchar(255) )

DECLARE @v varchar(20)
SELECT @v = [internal].[GetSQLServerVersion]()

IF (@v IN ('2005', '2008', '2008R2'))
BEGIN
	INSERT INTO @databases([name])
	SELECT d.[name]
		FROM sys.databases d
		WHERE d.[name] NOT IN ('tempdb')
		  AND d.[state_desc] = 'ONLINE'
END
ELSE
BEGIN
	INSERT INTO @databases([name])
	SELECT d.[name]
		FROM sys.databases d LEFT OUTER JOIN sys.dm_hadr_availability_replica_states rs
		  ON rs.replica_id = d.replica_id
		WHERE d.[name] NOT IN ('tempdb')
		  AND d.[state_desc] = 'ONLINE'
		  AND ISNULL(rs.role_desc, N'PRIMARY') = N'PRIMARY'
END
;


/*  */
--Loop all online databases except tempdb
DECLARE dbccpage CURSOR
	LOCAL STATIC FORWARD_ONLY READ_ONLY
	FOR SELECT [name] FROM @databases

OPEN dbccpage;
FETCH NEXT FROM dbccpage INTO @dbname;
WHILE @@FETCH_STATUS = 0
BEGIN
    /*  */
	SET @SQL = 'USE [' + @dbname +'];' +char(10)+char(13)
	SET @SQL = @SQL + 'DBCC PAGE ( ['+ @dbname +'], 1, 9, 3) WITH NO_INFOMSGS, TABLERESULTS;' +char(10)+char(13)

	BEGIN TRY
	--Get system information about DBCC commands
	INSERT INTO @temp
	EXEC (@SQL);

	/* Only rows with good results should be inserted. Let the latest value be #1. */
	INSERT INTO @dbccresults ([database_name], [dbccLastKnownGood], [RowNum] )
				       SELECT @dbname        , [Value]            , ROW_NUMBER() OVER (PARTITION BY [Field] ORDER BY [Value] DESC)
			FROM @temp
			WHERE [Field] = 'dbi_dbccLastKnownGood'
			  AND [Value] != '1900-01-01 00:00:00.000';
	END TRY
	BEGIN CATCH 
		SET @SQL = 'Failure running commands against ' + @dbname
		PRINT @SQL
	END CATCH

	SET @SQL = ''
	DELETE FROM @temp;

	FETCH NEXT FROM dbccpage INTO @dbname;
END

CLOSE dbccpage;
DEALLOCATE dbccpage;

/* Remove potential duplicate rows. Could happen on some SQL Server versions. */
DELETE FROM @dbccresults WHERE [RowNum] <> 1;

/*
  Get the databases and the Last known DBCC command 
  Use the data to merge the database_properties results table
*/
WITH database_properties AS (
SELECT [database_id]
     , [name]
     , [owner_sid]
     , [create_date]
     , [compatibility_level]
     , [collation_name]
     , [is_auto_close_on]
     , [is_auto_shrink_on]
     , [state_desc]
     , [recovery_model_desc]
     , [page_verify_option_desc]
     , fullbackup.[LastFullBackupTime]
     , diffbackup.[LastDiffBackupTime]
     , logbackup.[LastLogBackupTime]
     , LastKnownGoodDBCCTime = dbccresults.[dbccLastKnownGood]
FROM sys.databases d LEFT OUTER JOIN @dbccresults dbccresults ON dbccresults.[database_name] = d.[name] 
CROSS APPLY ( /* Find the last backup of type 'Full' */
      SELECT [LastFullBackupTime] = MAX(bus.[backup_finish_date]) 
      FROM dbo.backupset bus
      WHERE bus.[type] = 'D'
	  AND bus.[database_name] = d.[name]
      ) fullbackup
CROSS APPLY ( /* Find the last backup of type 'Differential' */
      SELECT [LastDiffBackupTime] = MAX(bus.[backup_finish_date]) 
      FROM dbo.backupset bus
      WHERE bus.[type] = 'I'
	  AND bus.[database_name] = d.[name]
      ) diffbackup
CROSS APPLY ( /* Find the last backup of type 'Log' */
      SELECT [LastLogBackupTime] = MAX(bus.[backup_finish_date]) 
      FROM dbo.backupset bus
      WHERE bus.[type] = 'L'
	  AND bus.[database_name] = d.[name]
      ) logbackup
)

/* Update the database metadata with the latest numbers */
MERGE [data].[database_properties] dest USING 
(SELECT [database_id] ,[name] ,[owner_sid] ,[create_date] ,[compatibility_level] ,[collation_name] ,[is_auto_close_on] ,[is_auto_shrink_on] ,[state_desc] ,[recovery_model_desc]
      ,[page_verify_option_desc] ,[LastFullBackupTime] ,[LastDiffBackupTime] ,[LastLogBackupTime] ,[LastKnownGoodDBCCTime]
  FROM database_properties) src ON src.[database_id] = dest.[database_id]
WHEN NOT MATCHED THEN
 INSERT ([database_id] ,[name] ,[owner_sid] ,[create_date] ,[compatibility_level] ,[collation_name] ,[is_auto_close_on] ,[is_auto_shrink_on] ,[state_desc] 
        ,[recovery_model_desc] ,[page_verify_option_desc] ,[LastFullBackupTime] ,[LastDiffBackupTime] ,[LastLogBackupTime] ,[LastKnownGoodDBCCTime] ,[LastUpdated])
 VALUES (src.[database_id] ,src.[name] ,src.[owner_sid] ,src.[create_date] ,src.[compatibility_level] ,src.[collation_name] ,src.[is_auto_close_on] ,src.[is_auto_shrink_on] ,src.[state_desc] 
        ,src.[recovery_model_desc] ,src.[page_verify_option_desc] ,src.[LastFullBackupTime] ,src.[LastDiffBackupTime] ,src.[LastLogBackupTime] ,src.[LastKnownGoodDBCCTime] ,SYSUTCDATETIME())
WHEN MATCHED THEN
 UPDATE SET 
      [name] = src.[name]
      ,[owner_sid] = src.[owner_sid]
      ,[create_date] = src.[create_date]
      ,[compatibility_level] = src.[compatibility_level]
      ,[collation_name] = src.[collation_name]
      ,[is_auto_close_on] = src.[is_auto_close_on]
      ,[is_auto_shrink_on] = src.[is_auto_shrink_on]
      ,[state_desc] = src.[state_desc]
      ,[recovery_model_desc] = src.[recovery_model_desc]
      ,[page_verify_option_desc] = src.[page_verify_option_desc]
      ,[LastFullBackupTime] = src.[LastFullBackupTime]
      ,[LastDiffBackupTime] = src.[LastDiffBackupTime]
      ,[LastLogBackupTime] = src.[LastLogBackupTime]
      ,[LastKnownGoodDBCCTime] = src.[LastKnownGoodDBCCTime]
      ,[LastUpdated] = SYSUTCDATETIME();
END
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
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	UPDATE s
	SET [LastHandled] = SYSUTCDATETIME()
	OUTPUT @serverid serverid 
	     , inserted.[database_id]
		 , inserted.[name]
		 , inserted.[owner_sid]
		 , inserted.[create_date]
		 , inserted.[compatibility_level]
		 , inserted.[collation_name]
		 , inserted.[is_auto_close_on]
		 , inserted.[is_auto_shrink_on]
		 , inserted.[state_desc]
		 , inserted.[recovery_model_desc]
		 , inserted.[page_verify_option_desc]
		 , inserted.[LastFullBackupTime]
		 , inserted.[LastDiffBackupTime]
		 , inserted.[LastLogBackupTime]
		 , inserted.[LastKnownGoodDBCCTime]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[database_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

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
		INSERT ([section], [collector], [cron], [lastrun])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01');
GO
