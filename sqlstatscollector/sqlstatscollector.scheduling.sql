USE [msdb]
GO

DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM dbo.syscategories WHERE name=N'sqlstatscollector' AND category_class=1)
BEGIN
	EXEC @ReturnCode = dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'sqlstatscollector'
END

EXEC msdb.dbo.sp_delete_job @job_name = N'Data collection for [sqlstatscollector]'

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  dbo.sp_add_job @job_name=N'Data collection for [sqlstatscollector]', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Collects data from local instance.', 
		@category_name=N'sqlstatscollector', 
		@owner_login_name=N'sa',
		@job_id = @jobId OUTPUT

EXEC @ReturnCode = dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Run the collection PROCEDURE', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [collect].[run]', 
		@database_name=N'$(SQLCMDDBNAME)', 
		@flags=0

EXEC @ReturnCode = dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

EXEC @ReturnCode = dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Collection Schedule for [sqlstatscollector]', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20220504, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959

EXEC @ReturnCode = dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

EXEC @ReturnCode = dbo.sp_update_job @job_id=@jobId, @enabled=0

GO

