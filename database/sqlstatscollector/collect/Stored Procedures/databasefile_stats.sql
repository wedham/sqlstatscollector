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
*******************************************************************************/
CREATE PROCEDURE [collect].[databasefile_stats]
AS
BEGIN
PRINT('[collect].[databasefile_stats] - Get all IO stats and file sizes of individual database files')
SET NOCOUNT ON

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


/*  */
DECLARE @sizeresults TABLE ([database_id] int 
                          , [file_id] int
						    , [used_pages] int)

DECLARE	@dbname sysname
DECLARE	@filename sysname
DECLARE	@database_id int
DECLARE	@file_id int
DECLARE @SQL varchar(512);

--/*  */
--Loop all online databases 
DECLARE spaceused CURSOR
	LOCAL STATIC FORWARD_ONLY READ_ONLY
	FOR SELECT d.[name], d.database_id, f.file_id, f.name
		FROM sys.databases d INNER JOIN sys.master_files f ON d.database_id = f.database_id
		WHERE d.[state_desc] = 'ONLINE'

OPEN spaceused;
FETCH NEXT FROM spaceused INTO @dbname, @database_id, @file_id, @filename;
WHILE @@FETCH_STATUS = 0
BEGIN
    /*  */
	SET @SQL = 'Use [' + @dbname +'];' +char(10)+char(13)
	SET @SQL = @SQL + 'SELECT ' 
	                + CAST(@database_id AS nvarchar(10)) 
					+ ', ' 
					+ CAST(@file_id AS nvarchar(10)) 
					+ ', CAST(FILEPROPERTY( '''+ @filename +''', ''SpaceUsed'') as int)' 
					+ char(10)+char(13)

	INSERT INTO @sizeresults ([database_id], [file_id], [used_pages] )
	EXEC (@SQL);

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
  LEFT OUTER JOIN [data_previous].[databasefile_stats] previousstats
  ON currentstats.[database_id] = previousstats.[database_id] AND currentstats.[file_id] = previousstats.[file_id]

TRUNCATE TABLE [data_previous].[databasefile_stats]
INSERT INTO [data_previous].[databasefile_stats]
		   ([database_id] ,[file_id] ,[size] ,[free_pages] ,[num_of_reads] ,[num_of_bytes_read] ,[io_stall_read_ms] ,[num_of_writes] ,[num_of_bytes_written] ,[io_stall_write_ms])
	 SELECT [database_id] ,[file_id] ,[size] ,[free_pages] ,[num_of_reads] ,[num_of_bytes_read] ,[io_stall_read_ms] ,[num_of_writes] ,[num_of_bytes_written] ,[io_stall_write_ms]
  FROM @databasefile_stats

END