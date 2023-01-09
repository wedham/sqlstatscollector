WITH XmlEvents AS (
SELECT event_data = CAST(event_data as xml)
,f.timestamp_utc
FROM sys.fn_xe_file_target_read_file(N'DetectDatabaseUsage*.xel', NULL, NULL, NULL) f
), ConnectionResults AS (
SELECT [db_name] = e.event_data.value('(/event/data[@name="database_name"]/value)[1]', 'nvarchar(128)') 
, [host_name] = e.event_data.value('(/event/action[@name="client_hostname"]/value)[1]', 'nvarchar(128)') 
, [login_name] = e.event_data.value('(/event/action[@name="username"]/value)[1]', 'nvarchar(128)') 
, [program_name] = e.event_data.value('(/event/action[@name="client_app_name"]/value)[1]', 'nvarchar(128)') 
, IsConnected = 1 
, e.timestamp_utc
, e.event_data
FROM XmlEvents e 
)
SELECT [db_name]
     , [host_name]
	 , [login_name]
	 , [program_name]
	 , [IsConnected]
	 , [last_seen] = MAX(timestamp_utc)
FROM ConnectionResults
GROUP BY [db_name], [host_name], [login_name], [program_name], [IsConnected]

