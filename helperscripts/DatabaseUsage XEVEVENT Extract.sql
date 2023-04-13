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


GO


WITH ParsedXEVENT AS (
SELECT [database_id] =  n.value('(data[@name="database_id"]/value)[1]', 'int')
      ,[host_name] = n.value('(action[@name="client_hostname"]/value)[1]', 'nvarchar(128)')
      ,[last_seen] = n.value('(@timestamp)[1]', 'datetime2')
      ,[program_name] = n.value('(action[@name="client_app_name"]/value)[1]', 'nvarchar(128)')
      ,nt_username = n.value('(action[@name="nt_username"]/value)[1]', 'nvarchar(128)')
      ,session_nt_username = n.value('(action[@name="session_nt_username"]/value)[1]', 'nvarchar(128)')
      ,username = n.value('(action[@name="username"]/value)[1]', 'nvarchar(128)')
FROM (SELECT CAST(event_data AS XML) event_data
	  FROM sys.fn_xe_file_target_read_file('C:\dblog\*',  'C:\dblog\DetectDatabaseUsage-sqlstatscollector_0_133249903584920000.xem', null, null)
	  ) ed
CROSS APPLY ed.event_data.nodes('event') q(n)
)
SELECT [db_name] = DB_NAME([database_id])
      ,[host_name]
	  ,[login_name] = [username]
	  ,[program_name]
	  ,[connection_count] = COUNT(*)
	  ,[last_seen] = MAX([last_seen])
FROM ParsedXEVENT
WHERE DB_NAME([database_id]) IS NOT NULL AND [program_name] NOT LIKE '%IntelliSense%'
GROUP BY DB_NAME([database_id]), [host_name], [username], [program_name]

