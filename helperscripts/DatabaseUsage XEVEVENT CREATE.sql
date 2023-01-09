CREATE EVENT SESSION [DetectDatabaseUsage [sqlstatscollector]]] 
ON SERVER 
ADD EVENT sqlserver.lock_acquired(SET collect_database_name=(1),collect_resource_description=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.session_nt_username,sqlserver.username)
    WHERE ([resource_type]='DATABASE' AND [database_id]>(4) AND [sqlserver].[is_system]=(0) AND ([owner_type]='SharedXactWorkspace' )))
ADD TARGET package0.event_file(SET filename=N'DetectDatabaseUsage [sqlstatscollector]')
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
GO

