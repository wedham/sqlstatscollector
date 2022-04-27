/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[job_stats]
   -----------------------------------------
   Get all statistics and run times of jobs and steps

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-21	Mikael Wedham		+Created v1
2022-04-27	Mikael Wedham		+Brackets and naming
*******************************************************************************/
CREATE PROCEDURE [collect].[job_stats]
AS
BEGIN
PRINT('[collect].[job_stats] - Get execution statistics and run times of jobs')
SET NOCOUNT ON
;
WITH [details] AS (
				SELECT [rowselector] = ROW_NUMBER() OVER (PARTITION BY h.[job_id] ORDER BY h.[run_date] DESC, h.[run_time] DESC)
				, h.[job_id]
				, h.[run_duration]
				FROM [msdb].[dbo].[sysjobhistory] h
				WHERE h.[step_id] = 0
				), [jobdurations] AS
				(
				SELECT [job_id]
				, [run_duration_avg] = CAST(AVG([run_duration] * 1.0) AS decimal(18,3))
				FROM [details]
				WHERE [rowselector] <= 50
				GROUP BY [job_id]
				)
	UPDATE jp
		SET [run_duration_avg] = d.[run_duration_avg]
		  , [LastUpdated] = SYSUTCDATETIME()
	FROM [data].[job_properties] jp
	LEFT JOIN [jobdurations] d
	  ON d.[job_id] = jp.[job_id]

END