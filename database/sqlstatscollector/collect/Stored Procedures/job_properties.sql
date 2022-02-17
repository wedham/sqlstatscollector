/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[job_properties]
   -----------------------------------------
   Get all defined jobs in the server

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [collect].[job_properties]
AS
BEGIN
PRINT('[collect].[job_properties] - Get all defined jobs in the server')
SET NOCOUNT ON

-- Get SQL Server Agent jobs and Category information (Query 9) (SQL Server Agent Jobs)
SELECT sj.name AS [Job Name], sj.[description] AS [Job Description], 
sc.name AS [CategoryName], SUSER_SNAME(sj.owner_sid) AS [Job Owner],
sj.date_created AS [Date Created], sj.[enabled] AS [Job Enabled], 
sj.notify_email_operator_id, sj.notify_level_email, h.run_status,
RIGHT(STUFF(STUFF(REPLACE(STR(h.run_duration, 7, 0), ' ', '0'), 4, 0, ':'), 7, 0, ':'),8) AS [Last Duration - HHMMSS],
CONVERT(DATETIME, RTRIM(h.run_date) + ' ' + STUFF(STUFF(REPLACE(STR(RTRIM(h.run_time),6,0),' ','0'),3,0,':'),6,0,':')) AS [Last Start Date]
FROM msdb.dbo.sysjobs AS sj WITH (NOLOCK)
INNER JOIN
    (SELECT job_id, instance_id = MAX(instance_id)
     FROM msdb.dbo.sysjobhistory WITH (NOLOCK)
     GROUP BY job_id) AS l
ON sj.job_id = l.job_id
INNER JOIN msdb.dbo.syscategories AS sc WITH (NOLOCK)
ON sj.category_id = sc.category_id
INNER JOIN msdb.dbo.sysjobhistory AS h WITH (NOLOCK)
ON h.job_id = l.job_id
AND h.instance_id = l.instance_id
ORDER BY CONVERT(INT, h.run_duration) DESC, [Last Start Date] DESC OPTION (RECOMPILE);
------

--run_status	
-- Value   Status of the job execution
-- 0 =     Failed
-- 1 =     Succeeded
-- 2 =     Retry
-- 3 =     Canceled
-- 4 =     In Progress

-- Gives you some basic information about your SQL Server Agent jobs, who owns them and how they are configured
-- Look for Agent jobs that are not owned by sa
-- Look for jobs that have a notify_email_operator_id set to 0 (meaning no operator)
-- Look for jobs that have a notify_level_email set to 0 (meaning no e-mail is ever sent)


END