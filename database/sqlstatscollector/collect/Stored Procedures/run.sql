/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[run]
   -----------------------------------------
   Entry point of collection database functionality.
   This procedure is the only one to be scheduled for data collection.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2022-05-11  Mikael Wedham		+Fixed scheduling issue
*******************************************************************************/
CREATE   PROCEDURE [collect].[run]
AS
BEGIN
PRINT(SYSUTCDATETIME())
PRINT('[collect].[run] - Begin collecting SQL Server statistics')
SET NOCOUNT ON
	DECLARE @current_collector varchar(100)
	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)

	IF OBJECT_ID('tempdb..#collectors') IS NOT NULL
	BEGIN
		DROP TABLE #collectors
	END

	CREATE TABLE #collectors (collector varchar(100))

	INSERT INTO #collectors(collector)
	SELECT c.collector
	FROM internal.collectors c
	CROSS APPLY cron.GetNextScheduleAfter(c.cron, c.lastrun) times
	WHERE times.scheduledtime < SYSUTCDATETIME()

	WHILE EXISTS (SELECT * FROM #collectors)
	BEGIN 
		DECLARE @worktable TABLE (collector varchar(100));

		WITH OneRow AS
		(SELECT TOP(1) collector FROM #collectors)
		DELETE FROM OneRow
		OUTPUT deleted.collector
		INTO @worktable;

		SELECT @current_collector = collector 
		FROM @worktable

		SELECT @current_start = SYSUTCDATETIME()

		DECLARE @current_logitem int
		INSERT INTO [internal].[executionlog] (collector, StartTime)
		SELECT @current_collector, @current_start
		SET @current_logitem = SCOPE_IDENTITY()

		BEGIN TRY
			EXEC ('EXEC [collect].[' + @current_collector + ']')
		END TRY
		BEGIN CATCH
			PRINT(ERROR_MESSAGE())
		END CATCH

		SELECT @current_end = SYSUTCDATETIME()

		UPDATE internal.executionlog
		SET EndTime = @current_end
		, Duration_ms =  (CAST(DATEDIFF(S, @current_start, @current_end) AS bigint) * 1000) + (DATEPART(MS, @current_end)-DATEPART(MS, @current_start))
		, errornumber = @@ERROR
		WHERE Id = @current_logitem

		UPDATE internal.collectors 
		SET lastrun = @current_end
		WHERE collector = @current_collector

	END
	PRINT('[collect].[run] - Finished collecting SQL Server statistics')
	PRINT(SYSUTCDATETIME())
END
