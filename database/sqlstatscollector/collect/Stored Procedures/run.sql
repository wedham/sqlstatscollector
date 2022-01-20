CREATE   PROCEDURE [collect].[run]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @current_collector varchar(100)

	IF OBJECT_ID('tempdb..#collectors') IS NOT NULL
	BEGIN
		DROP TABLE #collectors
	END

	CREATE TABLE #collectors ([collector] varchar(100))

	INSERT INTO #collectors([collector])
	SELECT c.[collector]
	FROM [internal].[collectors] c
	CROSS APPLY [cron].[GetPreviousSchedule](c.cron) times
	WHERE times.[scheduledtime] > c.lastrun

	WHILE EXISTS (SELECT * FROM #collectors)
	BEGIN 
		DECLARE @worktable TABLE ([collector] varchar(100));

		WITH OneRow AS
		(SELECT TOP(1) [collector] FROM #collectors)
		DELETE FROM OneRow
		OUTPUT deleted.[collector]
		INTO @worktable;

		SELECT @current_collector = [collector]
		FROM @worktable

		DECLARE @current_logitem int
		INSERT INTO [internal].[executionlog] ([collector], [StartTime])
		SELECT @current_collector, SYSDATETIME()
		SET @current_logitem = SCOPE_IDENTITY()

		BEGIN TRY
			EXEC ('EXEC [collect].[' + @current_collector + ']')
		END TRY
		BEGIN CATCH
			PRINT(ERROR_MESSAGE())
		END CATCH

		UPDATE [internal].[executionlog]
		SET [EndTime] = SYSDATETIME(), [Duration_ms] = DATEDIFF_BIG(MILLISECOND, [StartTime], SYSDATETIME()), [errornumber] = @@ERROR
		WHERE [Id] = @current_logitem

		UPDATE [internal].[collectors]
		SET [lastrun] = SYSDATETIME()
		WHERE [collector] = @current_collector

	END
END
