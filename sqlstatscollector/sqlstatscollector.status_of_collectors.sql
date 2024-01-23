SET NOCOUNT ON 
GO

RAISERROR(N'/****** Object:  StoredProcedure [internal].[collector_status] ******/', 10, 1) WITH NOWAIT
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[internal].[collector_status]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [internal].[collector_status] AS SELECT NULL')
END
GO

/*******************************************************************************
   Copyright (c) 2024 Mikael Wedham (MIT License)
   -----------------------------------------
   [internal].[collector_status]
   -----------------------------------------
   Enables or disables a collector.
   Has functionality to add resources needed for collectors

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-01-23	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [internal].[collector_status]
( @collector varchar(100)
, @enabled bit)
AS
BEGIN
SET NOCOUNT ON

	--Special handling for 'connection_properties' includes an Extended Events session
	IF @collector = 'connection_properties'
	BEGIN
		DECLARE @session_is_running int

		IF @enabled = 1
		BEGIN
			--Check for the XEvent session. If it does not exist, create it
			IF NOT EXISTS (SELECT * FROM sys.server_event_sessions WHERE [name] = N'sqlstatscollector-connection_properties')
			BEGIN	
				CREATE EVENT SESSION [sqlstatscollector-connection_properties] ON SERVER 
				ADD EVENT sqlserver.lock_acquired(
					ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.nt_username,sqlserver.session_nt_username,sqlserver.username)
					WHERE ([resource_type]=(2) AND [database_id]>(4) AND [sqlserver].[is_system]=(0) AND [owner_type]=(4)))
				ADD TARGET package0.ring_buffer(SET max_events_limit=(8192))
				WITH (EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS
					 ,MAX_DISPATCH_LATENCY=10 SECONDS
					 ,TRACK_CAUSALITY=OFF
					 ,STARTUP_STATE=ON)
			END

			SELECT @session_is_running = IIF(running.[name] IS NULL, 0, 1)
			FROM sys.server_event_sessions es LEFT OUTER JOIN sys.dm_xe_sessions running
			  ON es.[name] = running.[name]
			WHERE es.[name] = N'sqlstatscollector-connection_properties'

			--Start the session if it is not running
			IF @session_is_running = 0
			BEGIN
				ALTER EVENT SESSION [sqlstatscollector-connection_properties] ON SERVER 
				STATE = START;
			END
		END
		ELSE
		BEGIN
			--Check for the XEvent session. If it exists, clean up and remove it
			IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE [name] = N'sqlstatscollector-connection_properties')
			BEGIN
				DROP EVENT SESSION [sqlstatscollector-connection_properties] ON SERVER;
			END
		END

	END --@collector = 'connection_properties'

	UPDATE [internal].[collectors]
	SET [is_enabled] = @enabled
	WHERE [collector] = @collector


END
