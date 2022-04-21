

CREATE PROCEDURE [transfer].[databasefile_stats]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT serverid = @serverid
	     , database_id
		 , [file_id]
		 , size_mb
		 , freespace_mb
		 , num_of_reads
		 , num_of_bytes_read
		 , io_stall_read_ms
		 , num_of_writes
		 , num_of_bytes_written
		 , io_stall_write_ms
		 , rowtime
		 , LastHandled
	FROM [data].[databasefile_stats]
END