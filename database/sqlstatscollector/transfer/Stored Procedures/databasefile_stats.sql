
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
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
CREATE PROCEDURE [transfer].[databasefile_stats]
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