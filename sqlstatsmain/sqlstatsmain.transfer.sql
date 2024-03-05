SET NOCOUNT ON 
GO

RAISERROR(N'/****** Object:  StoredProcedure [transfer].[run] ******/', 10, 1) WITH NOWAIT
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[transfer].[run]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[run] AS SELECT NULL')
END
GO

/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[run]
   -----------------------------------------
   Entry point of collection database functionality.
   This procedure is the only one to be scheduled for data collection.

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-03-05  Mikael Wedham		+Created version 1
*******************************************************************************/
ALTER PROCEDURE [transfer].[run]
AS
BEGIN
PRINT(SYSUTCDATETIME())
PRINT('[transfer].[run] - Begin collecting SQL Server statistics')
SET NOCOUNT ON
	DECLARE @current_collector varchar(100)

	IF OBJECT_ID('tempdb..#collectors') IS NOT NULL
	BEGIN
		DROP TABLE #collectors
	END

	CREATE TABLE #collectors (collector varchar(100))

	INSERT INTO #collectors(collector)
	SELECT c.collector
	FROM internal.collectors c
	WHERE [is_enabled] = 1
	  AND cron.GetNext(c.cron, c.lastrun) < SYSUTCDATETIME()

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

		BEGIN TRY
			EXEC ('EXEC [transfer].[' + @current_collector + ']')
		END TRY
		BEGIN CATCH
			PRINT(ERROR_MESSAGE())
		END CATCH


		UPDATE internal.collectors 
		SET lastrun = SYSUTCDATETIME()
		WHERE collector = @current_collector

	END
	PRINT('[transfer].[run] - Finished collecting SQL Server statistics')
	PRINT(SYSUTCDATETIME())
END
GO


