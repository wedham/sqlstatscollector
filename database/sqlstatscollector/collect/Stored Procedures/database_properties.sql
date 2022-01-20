CREATE   PROCEDURE [collect].[database_properties]
AS
BEGIN
SET NOCOUNT ON
DECLARE @temp TABLE ([ParentObject] varchar(255)
                   , [Object] varchar(255)
				   , [Field] varchar(255)
				   , [Value] varchar(255) )

DECLARE @dbccresults TABLE ([database_name] sysname 
                          , [dbccLastKnownGood] datetime
						  , [RowNum] int)

DECLARE	@dbname sysname
DECLARE @SQL varchar(512);

DECLARE dbccpage CURSOR
	LOCAL STATIC FORWARD_ONLY READ_ONLY
	FOR SELECT [name]
		FROM sys.databases
		WHERE [name] NOT IN ('tempdb');

OPEN dbccpage;
FETCH NEXT FROM dbccpage INTO @DBName;
WHILE @@FETCH_STATUS = 0
BEGIN
	SET @SQL = 'Use [' + @DBName +'];' +char(10)+char(13)
	SET @SQL = @SQL + 'DBCC PAGE ( ['+ @DBName +'], 1, 9, 3) WITH TABLERESULTS;' +char(10)+char(13)

	INSERT INTO @temp
	EXEC (@SQL);

	INSERT INTO @dbccresults ([database_name], [dbccLastKnownGood], [RowNum] )
				   SELECT @dbname        , [Value]          , ROW_NUMBER() OVER (PARTITION BY [Field] ORDER BY [Value])
			FROM @temp
			WHERE [Field] = 'dbi_dbccLastKnownGood'
			  AND [Value] != '1900-01-01 00:00:00.000';

	SET @SQL = ''
	DELETE FROM @temp;

	FETCH NEXT FROM dbccpage INTO @DBName;
END

CLOSE dbccpage;
DEALLOCATE dbccpage;

--TODO make msdb access more efficient
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
     , fullbackup.LastFullBackupTime
     , diffbackup.LastDiffBackupTime
     , logbackup.LastLogBackupTime
     , LastKnownGoodDBCCTime = dbccresults.dbccLastKnownGood
FROM sys.databases d LEFT OUTER JOIN @dbccresults dbccresults ON dbccresults.[database_name] = d.[name] AND dbccresults.[RowNum] = 1
CROSS APPLY (
      SELECT LastFullBackupTime = MAX(bus.[backup_finish_date]) 
      FROM [msdb].[dbo].[backupset] bus
      WHERE bus.[type] = 'D'
	  AND bus.[database_name] = d.[name]
      ) fullbackup
CROSS APPLY (
      SELECT LastDiffBackupTime = MAX(bus.[backup_finish_date]) 
      FROM [msdb].[dbo].[backupset] bus
      WHERE bus.[type] = 'I'
	  AND bus.[database_name] = d.[name]
      ) diffbackup
CROSS APPLY (
      SELECT LastLogBackupTime = MAX(bus.[backup_finish_date]) 
      FROM [msdb].[dbo].[backupset] bus
      WHERE bus.[type] = 'L'
	  AND bus.[database_name] = d.[name]
      ) logbackup
)

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