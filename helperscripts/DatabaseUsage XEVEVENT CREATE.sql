CREATE EVENT SESSION [DetectDatabaseUsage [sqlstatscollector]]] 
ON SERVER 
ADD EVENT sqlserver.lock_acquired(SET collect_database_name=(1),collect_resource_description=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.session_nt_username,sqlserver.username)
    WHERE ([resource_type]='DATABASE' AND [database_id]>(4) AND [sqlserver].[is_system]=(0) AND ([owner_type]='SharedXactWorkspace' )))
ADD TARGET package0.event_file(SET filename=N'DetectDatabaseUsage [sqlstatscollector]')
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
GO

--FUNKAR PÅ 2008 också
----https://www.sqlskills.com/blogs/jonathan/tracking-sql-server-database-usage/
IF EXISTS (SELECT 1 
            FROM sys.server_event_sessions 
            WHERE name = 'DetectDatabaseUsage-sqlstatscollector')
    DROP EVENT SESSION [DetectDatabaseUsage-sqlstatscollector] 
    ON SERVER;
GO

CREATE EVENT SESSION [DetectDatabaseUsage-sqlstatscollector] ON SERVER 
ADD EVENT sqlserver.lock_acquired(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.nt_username,sqlserver.session_nt_username,sqlserver.username)
    WHERE ([resource_type]=(2) AND [database_id]>(4) AND [sqlserver].[is_system]=(0) AND [owner_type]=(4)))
ADD TARGET package0.asynchronous_file_target(SET filename=N'C:\dblog\DetectDatabaseUsage-sqlstatscollector')
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=1 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
GO

-- Start the Event Session
ALTER EVENT SESSION [DetectDatabaseUsage-sqlstatscollector] 
ON SERVER 
STATE = START;
GO

SELECT * FROM sys.fn_xe_file_target_read_file('C:\dblog\*.xel', null, null, null)

select
    n.value('(@name)[1]', 'varchar(50)') as event_name,
    n.value('(@package)[1]', 'varchar(50)') AS package_name,
    n.value('(@timestamp)[1]', 'datetime2') AS [utc_timestamp],
    n.value('(data[@name="database_id"]/value)[1]', 'int') as database_id,
    n.value('(action[@name="client_app_name"]/value)[1]', 'nvarchar(128)') as client_app_name,
    n.value('(action[@name="client_hostname"]/value)[1]', 'nvarchar(128)') as client_hostname,
    n.value('(action[@name="nt_username"]/value)[1]', 'nvarchar(128)') as nt_username,
    n.value('(action[@name="session_nt_username"]/value)[1]', 'nvarchar(128)') as session_nt_username,
    n.value('(action[@name="username"]/value)[1]', 'nvarchar(128)') as username

from (select cast(event_data as XML) as event_data
from sys.fn_xe_file_target_read_file('C:\dblog\*', 'C:\dblog\DetectDatabaseUsage-sqlstatscollector_0_133249819184420000.xem', null, null)) ed
cross apply ed.event_data.nodes('event') as q(n)





--*****************************************************
--*****************************************************
--*****************************************************
--*****************************************************
--RING BUFFER
--*****************************************************
--*****************************************************
--*****************************************************
--*****************************************************

-- Create an Event Session to capture Errors Reported
CREATE EVENT SESSION DemoPersistedEvents
ON SERVER
ADD EVENT sqlserver.error_reported
ADD TARGET package0.ring_buffer
WITH (MAX_DISPATCH_LATENCY = 1 SECONDS);
GO
-- Alter the Event Session and Start it.
ALTER EVENT SESSION DemoPersistedEvents
ON SERVER
STATE=START;
GO
-- SELECT from a non-existent table to create Event
SELECT *
FROM master.schema_doesnt_exist.table_doesnt_exist;
GO
-- Drop the Event to halt Event collection
ALTER EVENT SESSION DemoPersistedEvents
ON SERVER
DROP EVENT sqlserver.error_reported;
GO
-- Wait for Event buffering to Target
WAITFOR DELAY '00:00:01';
GO
-- Create XML variable to hold Target Data
DECLARE @target_data XML;
SELECT @target_data = CAST(target_data AS XML)
FROM sys.dm_xe_sessions AS s 
JOIN sys.dm_xe_session_targets AS t 
    ON t.event_session_address = s.address
WHERE s.name = N'DemoPersistedEvents'
  AND t.target_name = N'ring_buffer';
 
-- Query XML variable to get Event Data
SELECT
    @target_data.value('(RingBufferTarget/@eventsPerSec)[1]', 'int') AS eventsPerSec,
    @target_data.value('(RingBufferTarget/@processingTime)[1]', 'int') AS processingTime,
    @target_data.value('(RingBufferTarget/@totalEventsProcessed)[1]', 'int') AS totalEventsProcessed,
    @target_data.value('(RingBufferTarget/@eventCount)[1]', 'int') AS eventCount,
    @target_data.value('(RingBufferTarget/@droppedCount)[1]', 'int') AS droppedCount,
    @target_data.value('(RingBufferTarget/@memoryUsed)[1]', 'int') AS memoryUsed;
 
SELECT
    n.value('(@name)[1]', 'varchar(50)') AS event_name,
    n.value('(@package)[1]', 'varchar(50)') AS package_name,
    n.value('(@id)[1]', 'int') AS id,
    n.value('(@version)[1]', 'int') AS version,
    DATEADD(hh, 
            DATEDIFF(hh, GETUTCDATE(), CURRENT_TIMESTAMP), 
            n.value('(@timestamp)[1]', 'datetime2')) AS [timestamp],
    n.value('(data[@name="error"]/value)[1]', 'int') as error,
    n.value('(data[@name="severity"]/value)[1]', 'int') as severity,
    n.value('(data[@name="duration"]/value)[1]', 'int') as state,
    n.value('(data[@name="user_defined"]/value)[1]', 'varchar(5)') as user_defined,
    n.value('(data[@name="message"]/value)[1]', 'varchar(max)') as message
FROM @target_data.nodes('RingBufferTarget/event') AS q(n);
GO
-- Drop the Event Session to cleanup Demo
DROP EVENT SESSION DemoPersistedEvents
ON SERVER;

--****************************************************************************
--****************************************************************************
--****************************************************************************
--****************************************************************************
--****************************************************************************


DECLARE @ExtendedEventsSessionName sysname = N'<session_name_here>';
DECLARE @StartTime datetimeoffset;
DECLARE @EndTime datetimeoffset;
DECLARE @Offset int;
 
DROP TABLE IF EXISTS #xmlResults;
CREATE TABLE #xmlResults
(
      xeTimeStamp datetimeoffset NOT NULL
    , xeXML XML NOT NULL
);
 
SET @StartTime = DATEADD(HOUR, -4, GETDATE()); --modify this to suit your needs
SET @EndTime = GETDATE();
SET @Offset = DATEDIFF(MINUTE, GETDATE(), GETUTCDATE());
SET @StartTime = DATEADD(MINUTE, @Offset, @StartTime);
SET @EndTime = DATEADD(MINUTE, @Offset, @EndTime);
 
SELECT StartTimeUTC = CONVERT(varchar(30), @StartTime, 127)
    , StartTimeLocal = CONVERT(varchar(30), DATEADD(MINUTE, 0 - @Offset, @StartTime), 120)
    , EndTimeUTC = CONVERT(varchar(30), @EndTime, 127)
    , EndTimeLocal = CONVERT(varchar(30), DATEADD(MINUTE, 0 - @Offset, @EndTime), 120);
 
DECLARE @target_data xml;
SELECT @target_data = CONVERT(xml, target_data)
FROM sys.dm_xe_sessions AS s 
JOIN sys.dm_xe_session_targets AS t 
    ON t.event_session_address = s.address
WHERE s.name = @ExtendedEventsSessionName
    AND t.target_name = N'ring_buffer';
 
;WITH src AS 
(
    SELECT xeXML = xm.s.query('.')
    FROM @target_data.nodes('/RingBufferTarget/event') AS xm(s)
)
INSERT INTO #xmlResults (xeXML, xeTimeStamp)
SELECT src.xeXML
    , [xeTimeStamp] = src.xeXML.value('(/event/@timestamp)[1]', 'datetimeoffset(7)')
FROM src;
 
SELECT [TimeStamp] = CONVERT(varchar(30), DATEADD(MINUTE, 0 - @Offset, xr.xeTimeStamp), 120)
    , xr.xeXML
FROM #xmlResults xr
WHERE xr.xeTimeStamp >= @StartTime
    AND xr.xeTimeStamp<= @EndTime
ORDER BY xr.xeTimeStamp;