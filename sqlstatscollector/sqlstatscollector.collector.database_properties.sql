SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [database_properties]', 10, 1) WITH NOWAIT
GO

----------------------------------------------------------------
-- Table [data].[database_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[database_properties] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'database_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x3432D127FCEA32EBCDF1BB8EBC8D15EBCE01EACC8AEA0E6182B650FB07A88711

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
	SELECT @cmd = N'DROP TABLE ' + @FullName
	RAISERROR(@cmd, 10, 1) WITH NOWAIT
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
		[snapshot_isolation_state_desc] [nvarchar](60) NOT NULL,
		[is_read_committed_snapshot_on] [bit] NOT NULL,
		[is_trustworthy_on] [bit] NOT NULL,
		[is_query_store_on] [bit] NOT NULL,
		[is_encrypted] [bit] NOT NULL,
		[containment_desc] [nvarchar](60) NOT NULL,
		[LastFullBackupTime] [datetime] NULL,
		[LastDiffBackupTime] [datetime] NULL,
		[LastLogBackupTime] [datetime] NULL,
		[LastKnownGoodDBCCTime] [datetime] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_database_properties] PRIMARY KEY CLUSTERED 
			(
				[database_id] ASC, 
				[name] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

----------------------------------------------------------------
-- Table [data].[database_properties_changes]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[database_properties_changes] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'database_properties_changes'
DECLARE @TableDefinitionHash varbinary(32) = 0x231F3ECE8CB90275AC4DC626D94AF8688690682E4A86054A05974C04DEF0EDF2

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
	SELECT @cmd = N'DROP TABLE ' + @FullName
	RAISERROR(@cmd, 10, 1) WITH NOWAIT
	EXEC (@cmd)
	SET @TableExists = 0
END


IF @TableExists = 0
BEGIN
	SELECT @msg = N'Creating ' + @FullName
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	CREATE TABLE [data].[database_properties_changes](
		[rowtimeutc] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[propertyname] [nvarchar](128) NOT NULL,
		[old_value] [nvarchar](256) NOT NULL,
		[new_value] [nvarchar](256) NOT NULL,
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


----------------------------------------------------------------
-- Trigger [data].[database_properties_change]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Trigger [data].[database_properties_change] ******/', 10, 1) WITH NOWAIT
GO
CREATE OR ALTER TRIGGER [data].[database_properties_change]
ON [data].[database_properties]
AFTER UPDATE
AS
BEGIN

    INSERT INTO [data].[database_properties_changes] ([rowtimeutc], [database_id], [name], [propertyname], [old_value], [new_value])
    SELECT i.[LastUpdatedUTC], i.[database_id], i.[name], changedata.propertyname, changedata.old_value, changedata.new_value
    FROM inserted i INNER JOIN deleted d ON i.[database_id] = d.[database_id] AND i.[name] = d.[name] 
    CROSS APPLY ( VALUES 
    -- Insert a list of columns for change tracking here.

                  (N'[owner_sid]'                     , CAST(d.[owner_sid] AS nvarchar(256))                     , CAST(i.[owner_sid] AS nvarchar(256)))
                 ,(N'[create_date]'                   , CONVERT(nvarchar(256), d.[create_date], 121)             , CONVERT(nvarchar(256), i.[create_date], 121))
                 ,(N'[compatibility_level]'           , CAST(d.[compatibility_level] AS nvarchar(256))           , CAST(i.[compatibility_level] AS nvarchar(256)))
                 ,(N'[collation_name]'                , CAST(d.[collation_name] AS nvarchar(256))                , CAST(i.[collation_name] AS nvarchar(256)))
                 ,(N'[is_auto_close_on]'              , CAST(d.[is_auto_close_on] AS nvarchar(256))              , CAST(i.[is_auto_close_on] AS nvarchar(256)))
                 ,(N'[is_auto_shrink_on]'             , CAST(d.[is_auto_shrink_on] AS nvarchar(256))             , CAST(i.[is_auto_shrink_on] AS nvarchar(256)))
                 ,(N'[state_desc]'                    , CAST(d.[state_desc] AS nvarchar(256))                    , CAST(i.[state_desc] AS nvarchar(256)))
                 ,(N'[recovery_model_desc]'           , CAST(d.[recovery_model_desc] AS nvarchar(256))           , CAST(i.[recovery_model_desc] AS nvarchar(256)))
                 ,(N'[page_verify_option_desc]'       , CAST(d.[page_verify_option_desc] AS nvarchar(256))       , CAST(i.[page_verify_option_desc] AS nvarchar(256)))
                 ,(N'[snapshot_isolation_state_desc]' , CAST(d.[snapshot_isolation_state_desc] AS nvarchar(256)) , CAST(i.[snapshot_isolation_state_desc] AS nvarchar(256)))
                 ,(N'[is_read_committed_snapshot_on]' , CAST(d.[is_read_committed_snapshot_on] AS nvarchar(256)) , CAST(i.[is_read_committed_snapshot_on] AS nvarchar(256)))
                 ,(N'[is_trustworthy_on]'             , CAST(d.[is_trustworthy_on] AS nvarchar(256))             , CAST(i.[is_trustworthy_on] AS nvarchar(256)))
                 ,(N'[is_query_store_on]'             , CAST(d.[is_query_store_on] AS nvarchar(256))             , CAST(i.[is_query_store_on] AS nvarchar(256)))
                 ,(N'[is_encrypted]'                  , CAST(d.[is_encrypted] AS nvarchar(256))                  , CAST(i.[is_encrypted] AS nvarchar(256)))
                 ,(N'[containment_desc]'              , CAST(d.[containment_desc] AS nvarchar(256))              , CAST(i.[containment_desc] AS nvarchar(256)))
                 --,(N'[LastFullBackupTime]'            , CONVERT(nvarchar(256), d.[LastFullBackupTime], 121)      , CONVERT(nvarchar(256), i.[LastFullBackupTime], 121))
                 --,(N'[LastDiffBackupTime]'            , CONVERT(nvarchar(256), d.[LastDiffBackupTime], 121)      , CONVERT(nvarchar(256), i.[LastDiffBackupTime], 121))
                 --,(N'[LastLogBackupTime]'             , CONVERT(nvarchar(256), d.[LastLogBackupTime], 121)       , CONVERT(nvarchar(256), i.[LastLogBackupTime], 121))
                 --,(N'[LastKnownGoodDBCCTime]'         , CONVERT(nvarchar(256), d.[LastKnownGoodDBCCTime], 121)   , CONVERT(nvarchar(256), i.[LastKnownGoodDBCCTime], 121))

    --End of column list             
    ) changedata (propertyname ,old_value ,new_value)
    WHERE changedata.old_value <> changedata.new_value


END
GO


----------------------------------------------------------------
-- StoredProcedure [collect].[database_properties]
----------------------------------------------------------------
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
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-05-27	Mikael Wedham		Fix for reusable databaseids Issue #6
2026-06-03	Mikael Wedham		History functionality added
*******************************************************************************/
ALTER PROCEDURE [collect].[database_properties]
AS
BEGIN
PRINT('[collect].[database_properties] - Collection of metadata of databases.')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'database_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @temp TABLE ([ParentObject] varchar(255)
					, [Object] varchar(255)
					, [Field] varchar(255)
					, [Value] varchar(255) )

	DECLARE @dbccresults TABLE ([database_name] sysname 
							, [dbccLastKnownGood] datetime
							, [RowNum] int)

	DECLARE	@dbname sysname
	DECLARE @SQL varchar(512);

	DECLARE @databases TABLE ([name] nvarchar(255) )

	DECLARE @v varchar(20)

	BEGIN TRY

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


		/* Loop all online databases except tempdb */
		DECLARE dbccpage CURSOR
			LOCAL STATIC FORWARD_ONLY READ_ONLY
			FOR SELECT [name] FROM @databases

		OPEN dbccpage;
		FETCH NEXT FROM dbccpage INTO @dbname;
		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @SQL = 'USE [' + @dbname +'];' +char(10)+char(13)
			SET @SQL = @SQL + 'DBCC PAGE ( ['+ @dbname +'], 1, 9, 3) WITH NO_INFOMSGS, TABLERESULTS;' +char(10)+char(13)

			BEGIN TRY
			/* Get system information about DBCC commands */
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
			, [snapshot_isolation_state_desc]
			, [is_read_committed_snapshot_on]
			, [is_trustworthy_on]
			, [is_query_store_on]
			, [is_encrypted] 
			, [containment_desc]
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
			,[page_verify_option_desc], [snapshot_isolation_state_desc] , [is_read_committed_snapshot_on], [is_trustworthy_on], [is_query_store_on], [is_encrypted] 
			, [containment_desc] ,[LastFullBackupTime] ,[LastDiffBackupTime] ,[LastLogBackupTime] ,[LastKnownGoodDBCCTime]
		FROM database_properties) src ON src.[database_id] = dest.[database_id] AND src.[name] = dest.[name]
		WHEN NOT MATCHED THEN
		INSERT ([database_id] ,[name] ,[owner_sid] ,[create_date] ,[compatibility_level] ,[collation_name] ,[is_auto_close_on] ,[is_auto_shrink_on] ,[state_desc] 
				,[recovery_model_desc] ,[page_verify_option_desc], [snapshot_isolation_state_desc], [is_read_committed_snapshot_on], [is_trustworthy_on]
			    , [is_query_store_on], [is_encrypted], [containment_desc] ,[LastFullBackupTime] ,[LastDiffBackupTime] ,[LastLogBackupTime] ,[LastKnownGoodDBCCTime] ,[LastUpdatedUTC])
		VALUES (src.[database_id] ,src.[name] ,src.[owner_sid] ,src.[create_date] ,src.[compatibility_level] ,src.[collation_name] ,src.[is_auto_close_on] ,src.[is_auto_shrink_on] ,src.[state_desc] 
				,src.[recovery_model_desc] ,src.[page_verify_option_desc], src.[snapshot_isolation_state_desc], src.[is_read_committed_snapshot_on], src.[is_trustworthy_on]
			    , src.[is_query_store_on], src.[is_encrypted], src.[containment_desc] ,src.[LastFullBackupTime] ,src.[LastDiffBackupTime] ,src.[LastLogBackupTime] ,src.[LastKnownGoodDBCCTime] ,SYSUTCDATETIME())
		WHEN MATCHED THEN
		UPDATE SET 
			 [owner_sid] = src.[owner_sid]
			,[create_date] = src.[create_date]
			,[compatibility_level] = src.[compatibility_level]
			,[collation_name] = src.[collation_name]
			,[is_auto_close_on] = src.[is_auto_close_on]
			,[is_auto_shrink_on] = src.[is_auto_shrink_on]
			,[state_desc] = src.[state_desc]
			,[recovery_model_desc] = src.[recovery_model_desc]
			,[page_verify_option_desc] = src.[page_verify_option_desc]
			,[snapshot_isolation_state_desc] = src.[snapshot_isolation_state_desc]
			,[is_read_committed_snapshot_on] = src.[is_read_committed_snapshot_on]
			,[is_trustworthy_on] = src.[is_trustworthy_on]
			,[is_query_store_on] = src.[is_query_store_on]
			,[is_encrypted] = src.[is_encrypted]
			,[containment_desc] = src.[containment_desc]
			,[LastFullBackupTime] = src.[LastFullBackupTime]
			,[LastDiffBackupTime] = src.[LastDiffBackupTime]
			,[LastLogBackupTime] = src.[LastLogBackupTime]
			,[LastKnownGoodDBCCTime] = src.[LastKnownGoodDBCCTime]
			,[LastUpdatedUTC] = SYSUTCDATETIME();

	END TRY
	BEGIN CATCH
		DECLARE @msg nvarchar(4000)
		SELECT @error = ERROR_NUMBER(), @msg = ERROR_MESSAGE()
		PRINT (@msg)
	END CATCH

	SELECT @current_end = SYSUTCDATETIME()
	UPDATE [internal].[executionlog]
	SET [EndTimeUTC] = @current_end
	, [Duration_ms] =  ((CAST(DATEDIFF(S, @current_start, @current_end) AS bigint) * 1000000) + (DATEPART(MCS, @current_end)-DATEPART(MCS, @current_start))) / 1000.0
	, [errornumber] = @@ERROR
	WHERE [Id] = @current_logitem

END
GO

----------------------------------------------------------------
-- StoredProcedure [transfer].[database_properties]
----------------------------------------------------------------
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
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-06-03	Mikael Wedham		History functionality added
*******************************************************************************/
ALTER PROCEDURE [transfer].[database_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	DECLARE @database_properties TABLE (
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
		[snapshot_isolation_state_desc] [nvarchar](60) NOT NULL,
		[is_read_committed_snapshot_on] [bit] NOT NULL,
		[is_trustworthy_on] [bit] NOT NULL,
		[is_query_store_on] [bit] NOT NULL,
		[is_encrypted] [bit] NOT NULL,
		[containment_desc] [nvarchar](60) NOT NULL,
		[LastFullBackupTime] [datetime] NULL,
		[LastDiffBackupTime] [datetime] NULL,
		[LastLogBackupTime] [datetime] NULL,
		[LastKnownGoodDBCCTime] [datetime] NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL)



	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
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
		 , inserted.[snapshot_isolation_state_desc]
		 , inserted.[is_read_committed_snapshot_on]
		 , inserted.[is_trustworthy_on]
		 , inserted.[is_query_store_on]
		 , inserted.[is_encrypted]
		 , inserted.[containment_desc]
		 , inserted.[LastFullBackupTime]
		 , inserted.[LastDiffBackupTime]
		 , inserted.[LastLogBackupTime]
		 , inserted.[LastKnownGoodDBCCTime]
		 , inserted.[LastUpdatedUTC]
		 , inserted.[LastHandledUTC]
    INTO @database_properties
	FROM [data].[database_properties] s
	WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]

	SELECT dp.serverid 
	     , dp.[database_id]
		 , dp.[name]
		 , dp.[owner_sid]
		 , dp.[create_date]
		 , dp.[compatibility_level]
		 , dp.[collation_name]
		 , dp.[is_auto_close_on]
		 , dp.[is_auto_shrink_on]
		 , dp.[state_desc]
		 , dp.[recovery_model_desc]
		 , dp.[page_verify_option_desc]
		 , dp.[snapshot_isolation_state_desc]
		 , dp.[is_read_committed_snapshot_on]
		 , dp.[is_trustworthy_on]
		 , dp.[is_query_store_on]
		 , dp.[is_encrypted]
		 , dp.[containment_desc]
		 , dp.[LastFullBackupTime]
		 , dp.[LastDiffBackupTime]
		 , dp.[LastLogBackupTime]
		 , dp.[LastKnownGoodDBCCTime]
		 , dp.[LastUpdatedUTC]
		 , dp.[LastHandledUTC]
    FROM @database_properties dp

END
GO

----------------------------------------------------------------
-- Finalizing [database_properties]
----------------------------------------------------------------
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
