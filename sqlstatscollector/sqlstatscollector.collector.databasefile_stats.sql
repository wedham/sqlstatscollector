SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [databasefile_stats]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'databasefile_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0xADD974FE74859E382063032C53ED439DA4C4E55DCBBCBF989B61C054549B5263

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
	CREATE TABLE [data].[databasefile_stats](
		[rowtime] [datetime2](7) NOT NULL,
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[size_mb] [decimal](19, 4) NOT NULL,
		[freespace_mb] [decimal](19, 4) NOT NULL,
		[num_of_reads] [bigint] NOT NULL,
		[num_of_bytes_read] [bigint] NOT NULL,
		[io_stall_read_ms] [bigint] NOT NULL,
		[num_of_writes] [bigint] NOT NULL,
		[num_of_bytes_written] [bigint] NOT NULL,
		[io_stall_write_ms] [bigint] NOT NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		CONSTRAINT PK_data_databasefile_stats PRIMARY KEY CLUSTERED 
			(
				rowtime ASC,
				database_id ASC,
				file_id ASC
			) ON [PRIMARY]	
		) ON [PRIMARY]
END
GO

DECLARE @SchemaName nvarchar(128) = N'internal_data'
DECLARE @TableName nvarchar(128) = N'databasefile_stats'
DECLARE @TableDefinitionHash varbinary(32) = 0x3E4E10305C6101C93A06EF659BB353C6BBA206DFC2C11E628D8479B191BB3578

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
	CREATE TABLE [internal_data].[databasefile_stats](
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[size] [int] NOT NULL,
		[free_pages] [int] NOT NULL,
		[num_of_reads] [bigint] NOT NULL,
		[num_of_bytes_read] [bigint] NOT NULL,
		[io_stall_read_ms] [bigint] NOT NULL,
		[num_of_writes] [bigint] NOT NULL,
		[num_of_bytes_written] [bigint] NOT NULL,
		[io_stall_write_ms] [bigint] NOT NULL
		CONSTRAINT PK_internal_data_databasefile_stats PRIMARY KEY CLUSTERED 
			(
				database_id ASC,
				file_id ASC
			) ON [PRIMARY]	
	) ON [PRIMARY]
END




RAISERROR(N'/****** Object:  StoredProcedure [collect].[databasefile_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[databasefile_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[databasefile_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[databasefile_stats]
   -----------------------------------------
   Get all IO stats and file sizes of individual database files.
   Historical data on all fields.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2022-04-28	Mikael Wedham		+Modified Schema of temp-tables
2022-05-04	Mikael Wedham		+Try/Catch for DBCC where database 
                                 is unavailable due to AOAG
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
*******************************************************************************/
ALTER PROCEDURE [collect].[databasefile_stats]
AS
BEGIN
PRINT('[collect].[databasefile_stats] - Get all IO stats and file sizes of individual database files')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
	VALUES (N'databasefile_stats', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @databasefile_stats TABLE ([database_id] int NOT NULL
									,[file_id] int NOT NULL
									,[size] [int] NOT NULL
									,[free_pages] [int] NOT NULL
									,[num_of_reads] [bigint] NOT NULL
									,[num_of_bytes_read] [bigint] NOT NULL
									,[io_stall_read_ms] [bigint] NOT NULL
									,[num_of_writes] [bigint] NOT NULL
									,[num_of_bytes_written] [bigint] NOT NULL
									,[io_stall_write_ms] [bigint] NOT NULL)


	DECLARE @sizeresults TABLE ([database_id] int 
							, [file_id] int
								, [used_pages] int)

	DECLARE	@dbname sysname
	DECLARE	@filename sysname
	DECLARE	@database_id int
	DECLARE	@file_id int
	DECLARE @SQL varchar(512);

	DECLARE @databases TABLE ([name] sysname, [database_id] int, [file_id] int, [file_name] sysname )

	DECLARE @v varchar(20)

	BEGIN TRY

		SELECT @v = [internal].[GetSQLServerVersion]()

		IF (@v IN ('2005', '2008', '2008R2'))
		BEGIN
			INSERT INTO @databases([name] , [database_id] , [file_id] , [file_name])
			SELECT d.[name], d.[database_id], f.[file_id], f.[name]
				FROM sys.databases d INNER JOIN sys.master_files f ON d.[database_id] = f.[database_id]
				WHERE d.[state_desc] = 'ONLINE'
		END
		ELSE
		BEGIN
			INSERT INTO @databases([name] , [database_id] , [file_id] , [file_name])
			SELECT d.[name], d.[database_id], f.[file_id], f.[name]
				FROM sys.databases d INNER JOIN sys.master_files f ON d.[database_id] = f.[database_id]
				LEFT OUTER JOIN sys.dm_hadr_availability_replica_states rs
				ON rs.[replica_id] = d.[replica_id]
				WHERE d.[state_desc] = 'ONLINE'
				AND ISNULL(rs.[role_desc], N'PRIMARY') = N'PRIMARY'
		END
		;


		--/* Loop all online databases */
		DECLARE spaceused CURSOR
			LOCAL STATIC FORWARD_ONLY READ_ONLY
			FOR SELECT [name] , [database_id] , [file_id] , [file_name]
				FROM @databases

		OPEN spaceused;
		FETCH NEXT FROM spaceused INTO @dbname, @database_id, @file_id, @filename;
		WHILE @@FETCH_STATUS = 0
		BEGIN
			BEGIN TRY
			SET @SQL = 'Use [' + @dbname +'];' +char(10)+char(13)
			SET @SQL = @SQL + 'SELECT ' 
							+ CAST(@database_id AS nvarchar(10)) 
							+ ', ' 
							+ CAST(@file_id AS nvarchar(10)) 
							+ ', CAST(FILEPROPERTY( '''+ @filename +''', ''SpaceUsed'') as int)' 
							+ char(10)+char(13)

			INSERT INTO @sizeresults ([database_id], [file_id], [used_pages] )
			EXEC (@SQL);
			END TRY
			BEGIN CATCH
			END CATCH

			SET @SQL = ''

			FETCH NEXT FROM spaceused INTO @dbname, @database_id, @file_id, @filename;
		END

		CLOSE spaceused;
		DEALLOCATE spaceused;

		WITH io_virtual_file_stats AS(
		SELECT [database_id]
			, [file_id]
			, num_of_reads
			, num_of_bytes_read
			, io_stall_read_ms
			, num_of_writes
			, num_of_bytes_written
			, io_stall_write_ms 
	
		FROM sys.dm_io_virtual_file_stats(NULL, NULL))
		INSERT INTO @databasefile_stats ([database_id] ,[file_id] ,[size] ,[free_pages] 
										,[num_of_reads] ,[num_of_bytes_read] ,[io_stall_read_ms] 
										,[num_of_writes] ,[num_of_bytes_written] ,[io_stall_write_ms])
		SELECT f.[database_id]
			, f.[file_id]
			, [size] = f.[size]
			, [free_pages] = (f.[size] - s.[used_pages])
			, fs.num_of_reads
			, fs.num_of_bytes_read
			, fs.io_stall_read_ms
			, fs.num_of_writes
			, fs.num_of_bytes_written
			, fs.io_stall_write_ms 
		FROM sys.master_files f INNER JOIN @sizeresults s
		ON s.[database_id] = f.[database_id] AND s.[file_id] = f.[file_id]
		INNER JOIN io_virtual_file_stats fs
		ON f.[database_id] = fs.[database_id] AND f.[file_id] = fs.[file_id] 

		INSERT INTO [data].[databasefile_stats]
				([database_id]
				,[file_id]
				,[size_mb] 
				,[freespace_mb]
				,[num_of_reads]
				,[num_of_bytes_read]
				,[io_stall_read_ms]
				,[num_of_writes]
				,[num_of_bytes_written]
				,[io_stall_write_ms]
				,[LastUpdated]
				,[rowtime])
			SELECT [database_id] = currentstats.[database_id] 
				,[file_id] = currentstats.[file_id] 
				,[size_mb] = CAST((currentstats.[size] / 128.0 ) as decimal(19,4))
				,[freespace_mb] = CAST((currentstats.[free_pages] / 128.0) as decimal(19,4))
				,[num_of_reads] = currentstats.[num_of_reads] - ISNULL(previousstats.[num_of_reads], 0)
				,[num_of_bytes_read] = currentstats.[num_of_bytes_read]  - ISNULL(previousstats.[num_of_bytes_read], 0)
				,[io_stall_read_ms] = currentstats.[io_stall_read_ms]  - ISNULL(previousstats.[io_stall_read_ms], 0)
				,[num_of_writes] = currentstats.[num_of_writes] - ISNULL(previousstats.[num_of_writes], 0) 
				,[num_of_bytes_written] = currentstats.[num_of_bytes_written] - ISNULL(previousstats.[num_of_bytes_written], 0) 
				,[io_stall_write_ms] = currentstats.[io_stall_write_ms] - ISNULL(previousstats.[io_stall_write_ms], 0)
				,[rowtime] = SYSUTCDATETIME()
				,[LastUpdated] = SYSUTCDATETIME()
		FROM @databasefile_stats currentstats
		LEFT OUTER JOIN [internal_data].[databasefile_stats] previousstats
		ON currentstats.[database_id] = previousstats.[database_id] AND currentstats.[file_id] = previousstats.[file_id]

		TRUNCATE TABLE [internal_data].[databasefile_stats]
		INSERT INTO [internal_data].[databasefile_stats]
				([database_id] ,[file_id] ,[size] ,[free_pages] ,[num_of_reads] ,[num_of_bytes_read] ,[io_stall_read_ms] ,[num_of_writes] ,[num_of_bytes_written] ,[io_stall_write_ms])
			SELECT [database_id] ,[file_id] ,[size] ,[free_pages] ,[num_of_reads] ,[num_of_bytes_read] ,[io_stall_read_ms] ,[num_of_writes] ,[num_of_bytes_written] ,[io_stall_write_ms]
		FROM @databasefile_stats

	END TRY
	BEGIN CATCH
		DECLARE @msg nvarchar(4000)
		SELECT @error = ERROR_NUMBER(), @msg = ERROR_MESSAGE()
		PRINT (@msg)
	END CATCH

	SELECT @current_end = SYSUTCDATETIME()
	UPDATE [internal].[executionlog]
	SET [EndTime] = @current_end
	, [Duration_ms] =  ((CAST(DATEDIFF(S, @current_start, @current_end) AS bigint) * 1000000) + (DATEPART(MCS, @current_end)-DATEPART(MCS, @current_start))) / 1000.0
	, [errornumber] = @@ERROR
	WHERE [Id] = @current_logitem


END
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[databasefile_stats] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[databasefile_stats]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[databasefile_stats] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[databasefile_stats]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2022-08-17	Mikael Wedham		Added cleanup of old data
*******************************************************************************/
ALTER PROCEDURE [transfer].[databasefile_stats]
(@cleanup bit = 0)
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
		 , inserted.[rowtime]
	     , inserted.[database_id]
		 , inserted.[file_id]
		 , inserted.[size_mb]
		 , inserted.[freespace_mb]
		 , inserted.[num_of_reads]
		 , inserted.[num_of_bytes_read]
		 , inserted.[io_stall_read_ms]
		 , inserted.[num_of_writes]
		 , inserted.[num_of_bytes_written]
		 , inserted.[io_stall_write_ms]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[databasefile_stats] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

	IF @cleanup = 1
	BEGIN
		DELETE FROM [data].[databasefile_stats]
		WHERE [LastHandled] < DATEADD(DAY, -7, GETDATE())
	END

END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'databasefile_stats', '*/10 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
