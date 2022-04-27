/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[server_stats]
   -----------------------------------------
   Collecting running information and parameters on server/instance level 
   Page Life Expectancy and other Performance Monitor counters

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-21	Mikael Wedham		+Created v1
2022-04-27	Mikael Wedham		+Managed null values when initializing
                                 Moved PLE to memory collection
*******************************************************************************/
CREATE PROCEDURE [collect].[server_stats]
AS
BEGIN
PRINT('[collect].[server_stats] - Collecting running information and parameters on server')
SET NOCOUNT ON

	DECLARE @user_connections int
	DECLARE @batch_requests_sec int
	DECLARE @previous_batch_requests_sec int
	DECLARE @batch_request_count int

	SELECT @user_connections = ISNULL([cntr_value] , 0)
	FROM sys.dm_os_performance_counters WITH (NOLOCK)
	WHERE [object_name] LIKE N'%General Statistics%' 
	AND [counter_name] = N'User Connections'

	SELECT @batch_requests_sec = ISNULL([cntr_value] , 0)
	FROM sys.dm_os_performance_counters WITH (NOLOCK)
	WHERE [object_name] LIKE N'%SQL Statistics%' 
	AND [counter_name] = N'Batch Requests/sec'

	SELECT @previous_batch_requests_sec = ISNULL([batch_requests_sec], 0)
	FROM [internal].[server_stats]

	SELECT @batch_request_count = @batch_requests_sec - @previous_batch_requests_sec

	IF @batch_request_count < 0
	BEGIN
		--If counter was reset/restarted then begin with a new value
		SELECT @batch_request_count = ISNULL(@batch_requests_sec, 0)
	END

	INSERT INTO [data].[server_stats] ([user_connections], [batch_requests_sec], [rowtime], [LastUpdated])
								SELECT @user_connections , @batch_request_count, SYSUTCDATETIME(), SYSUTCDATETIME()

	TRUNCATE TABLE [internal].[server_stats]

	INSERT INTO [internal].[server_stats] ([batch_requests_sec])
    VALUES (@batch_requests_sec)

END