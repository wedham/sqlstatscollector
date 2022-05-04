/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
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
CREATE   PROCEDURE [collect].[database_properties]
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