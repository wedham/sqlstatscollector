

CREATE PROCEDURE [transfer].[databasefile_properties]
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
		 , [type_desc]
		 , [name]
		 , physical_name
		 , state_desc
		 , size_mb
		 , max_size_mb
		 , growth_mb
		 , growth_percent
		 , LastUpdated
		 , LastHandled
	FROM [data].[databasefile_properties]
END