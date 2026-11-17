SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [index_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'index_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x0C6DBDEBCCD3DA7696E495977D7C79BEBF2B28FF80E5732EED02A72A492EB1DD

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
	CREATE TABLE [data].[index_stats](
		[rowtimeutc] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[object_id] [int] NOT NULL,
		[index_id] [int] NOT NULL,
		[user_seeks] [bigint] NOT NULL,
		[user_scans] [bigint] NOT NULL,
		[user_lookups] [bigint] NOT NULL,
		[user_updates] [bigint] NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		CONSTRAINT PK_data_index_stats PRIMARY KEY CLUSTERED 
			(
				[rowtimeutc] ASC,
				[database_id] ASC,
				[object_id] ASC,
				[index_id] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'internal_data'
DECLARE @TableName nvarchar(128) = N'index_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x8B2DEFA069081CF51CC0FD11B1408E77A2D030E450DBBDD8C67420D2E71F6F4F

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
	CREATE TABLE [internal_data].[index_stats](
		[database_id] [int] NOT NULL,
		[object_id] [int] NOT NULL,
		[index_id] [int] NOT NULL,
		[user_seeks] [bigint] NOT NULL,
		[user_scans] [bigint] NOT NULL,
		[user_lookups] [bigint] NOT NULL,
		[user_updates] [bigint] NOT NULL
		CONSTRAINT PK_internal_data_index_stats PRIMARY KEY CLUSTERED 
			(
				[database_id] ASC,
				[object_id] ASC,
				[index_id] ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


RAISERROR(N'/****** Object:  StoredProcedure [collect].[index_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[index_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[index_stats] AS SELECT NULL')
END
GO



/*******************************************************************************
   Copyright (c) 2026 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[index_stats]
   -----------------------------------------
   Collecting index_stats for SQL Server.
   Collects the interval usage of indexes from the 
   system view sys.dm_db_index_usage_stats.

Date		Name				Description
----------	-------------		-----------------------------------------------
2026-11-17	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [collect].[index_stats]
AS
BEGIN
PRINT('[collect].[index_stats] - Collecting index usage stats')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'index_stats', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @index_stats TABLE ([database_id] [int] NOT NULL,
								[object_id] [int] NOT NULL,
								[index_id] [int] NOT NULL,
								[user_seeks] [bigint] NOT NULL,
								[user_scans] [bigint] NOT NULL,
								[user_lookups] [bigint] NOT NULL,
								[user_updates] [bigint] NOT NULL
								)

	DECLARE	@dbname sysname
	DECLARE @SQL varchar(2048);
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
		DECLARE databaselist CURSOR
			LOCAL STATIC FORWARD_ONLY READ_ONLY
			FOR SELECT [name] FROM @databases
		
		OPEN databaselist;
		FETCH NEXT FROM databaselist INTO @dbname;
		WHILE @@FETCH_STATUS = 0
		BEGIN

			SET @SQL = 'USE [' + @dbname +'];' + char(10) + char(13)
			SET @SQL = @SQL + 
			  'SELECT [database_id] = s.[database_id]
				, [object_id] = s.[object_id]
				, [index_id] = s.[index_id]
				, [user_seeks] = ISNULL(s.[user_seeks], 0)
				, [user_scans] = ISNULL(s.[user_scans], 0)
				, [user_lookups] = ISNULL(s.[user_lookups], 0)
				, [user_updates] = ISNULL(s.[user_updates], 0)
				FROM sys.dm_db_index_usage_stats s WITH(NOLOCK)
				WHERE OBJECTPROPERTY(s.[object_id],''IsUserTable'') = 1 
				AND s.[database_id] = DB_ID()' 
			+ char(10) + char(13)

			BEGIN TRY

				/* Get system information about indexes */
				INSERT INTO @index_stats ([database_id], [object_id], [index_id], [user_seeks], [user_scans], [user_lookups], [user_updates])
				EXEC (@SQL);

			END TRY
			BEGIN CATCH 
				SET @SQL = 'Failure running commands against ' + @dbname + error_message()
				PRINT @SQL
			END CATCH

			SET @SQL = ''

			FETCH NEXT FROM databaselist INTO @dbname;
		END




		INSERT INTO [data].[index_stats] ([rowtimeutc] ,[database_id], [object_id], [index_id], [user_seeks], [user_scans], [user_lookups], [user_updates], [LastUpdatedUTC])
		SELECT [rowtimeutc] = SYSUTCDATETIME()
			 , [database_id] = t_istat.[database_id]
			 , [object_id] = t_istat.[object_id]
			 , [index_id] = t_istat.[index_id]
			 , [user_seeks] = t_istat.[user_seeks] - ISNULL(i_istat.[user_seeks], 0)
			 , [user_scans] = t_istat.[user_scans] - ISNULL(i_istat.[user_scans], 0)
			 , [user_lookups] = t_istat.[user_lookups] - ISNULL(i_istat.[user_lookups], 0)
			 , [user_updates] = t_istat.[user_updates] - ISNULL(i_istat.[user_updates], 0)
			 , [LastUpdatedUTC] = SYSUTCDATETIME()
		FROM @index_stats t_istat
		LEFT OUTER JOIN [internal_data].[index_stats] i_istat
		ON t_istat.[database_id] = i_istat.[database_id]
		AND t_istat.[object_id] = i_istat.[object_id]
		AND t_istat.[index_id] = i_istat.[index_id]
		WHERE (t_istat.[user_seeks] - ISNULL(i_istat.[user_seeks], 0)) > 0
		   OR (t_istat.[user_scans] - ISNULL(i_istat.[user_scans], 0)) > 0
		   OR (t_istat.[user_lookups] - ISNULL(i_istat.[user_lookups], 0)) > 0
		   OR (t_istat.[user_updates] - ISNULL(i_istat.[user_updates], 0)) > 0


		TRUNCATE TABLE [internal_data].[index_stats]

		INSERT INTO [internal_data].[index_stats] ([database_id], [object_id], [index_id], [user_seeks], [user_scans], [user_lookups], [user_updates])
		SELECT [database_id], [object_id], [index_id], [user_seeks], [user_scans], [user_lookups], [user_updates]
		FROM @index_stats

		DECLARE @missing_data int;
		WITH missing_properties AS
		(
			SELECT istat.[database_id], istat.[object_id], istat.[index_id]
			FROM [data].[index_stats] istat
			EXCEPT
			SELECT iprop.[database_id], iprop.[object_id], iprop.[index_id] 
			FROM [data].[index_properties] iprop
		)

		SELECT @missing_data = COUNT(*)
		FROM missing_properties

		IF @missing_data > 0
		BEGIN
			UPDATE [internal].[collectors]
			SET [lastrun] = '2000-01-01'
			WHERE [collector] = N'index_properties'
		END

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


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[index_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[index_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[index_stats] AS SELECT NULL')
END
GO



/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[index_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2026-11-17	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[index_stats]
(@cleanup bit = 0)
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
	OUTPUT @serverid serverid 
	     , inserted.[rowtimeutc]
		 , inserted.[database_id] 
		 , inserted.[object_id]					 
		 , inserted.[index_id] 
		 , inserted.[user_seeks] 
		 , inserted.[user_scans] 
		 , inserted.[user_lookups] 
		 , inserted.[user_updates] 
		 , inserted.[LastUpdatedUTC]
		 , inserted.[LastHandledUTC]
	FROM [data].[index_stats] s
	WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]
	
	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[index_stats]
		WHERE [LastHandledUTC] < DATEADD(DAY, -7, GETDATE())
	END
END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'usage', 'index_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 0);
GO
