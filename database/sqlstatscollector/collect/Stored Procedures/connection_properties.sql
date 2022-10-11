
CREATE   PROCEDURE [collect].[connection_properties]
AS
BEGIN
	MERGE [data].[connection_properties] dest
	USING (
			SELECT DISTINCT [db_name] = ISNULL( DB_NAME([dbid]), N'')
						  , [host_name] = ISNULL( [hostname], N'')
						  , [login_name] = ISNULL( [loginame], N'')
						  , [program_name] = ISNULL( [program_name], N'')
						  , [last_seen] = SYSUTCDATETIME()
			FROM sys.sysprocesses
			WHERE spid > 50
		 ) src
	ON dest.[db_name] = src.[db_name] COLLATE Finnish_Swedish_CI_AS
		AND dest.[host_name] = src.[host_name] COLLATE Finnish_Swedish_CI_AS
		AND dest.[login_name] = src.[login_name] COLLATE Finnish_Swedish_CI_AS
		AND dest.[program_name] = src.[program_name] COLLATE Finnish_Swedish_CI_AS
	WHEN NOT MATCHED THEN
		INSERT ([db_name], [host_name], [login_name], [program_name], [connection_count], [last_seen])
		VALUES (src.[db_name], src.[host_name], src.[login_name], src.[program_name], 1, src.[last_seen])
	WHEN MATCHED THEN 
		UPDATE SET dest.[last_seen] = src.[last_seen]
		, dest.[connection_count] = dest.[connection_count] + 1
	;
END