/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[job_properties]
   -----------------------------------------
   Get all defined jobs in the server

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-21	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [collect].[job_properties]
AS
BEGIN
PRINT('[collect].[job_properties] - Get all defined jobs in the server')
SET NOCOUNT ON
;
MERGE [data].[job_properties] dest
USING (
SELECT job_id = sj.job_id
     , job_name = sj.[name]
	 , [description] = sj.[description] 
	 , job_category = sc.[name]
	 , job_owner = ISNULL(SUSER_SNAME(sj.owner_sid), CAST(sj.owner_sid as nvarchar(255)))
	 , [enabled] = sj.[enabled]
	 , notify_email_desc = CASE WHEN notify_email_operator_id = 0 OR notify_level_email = 0 THEN 'NONE'
							WHEN notify_level_email = 1 THEN 'SUCCESS'
							WHEN notify_level_email = 2 THEN 'FAILURE'
							WHEN notify_level_email = 3 THEN 'COMPLETION'
							END
	 , run_status_desc = CASE h.run_status WHEN 0 THEN 'FAILED'
										   WHEN 1 THEN 'SUCCEEDED'
										   WHEN 2 THEN 'RETRY'
										   WHEN 3 THEN 'CANCELLED'
										   WHEN 4 THEN 'IN PROGRESS'
										   END
	 , last_duration = h.run_duration
	 , last_startdate = CONVERT(DATETIME, RTRIM(h.run_date) + ' ' + STUFF(STUFF(REPLACE(STR(RTRIM(h.run_time),6,0),' ','0'),3,0,':'),6,0,':'))
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
) src
ON src.job_id = dest.job_id
WHEN NOT MATCHED THEN
INSERT     ([job_id]
           ,[job_name]
           ,[description]
           ,[job_category]
           ,[job_owner]
           ,[enabled]
           ,[notify_email_desc]
           ,[run_status_desc]
           ,[last_duration]
           ,[last_startdate]
		   ,[run_duration_avg]
           ,[LastUpdated])
     VALUES
           (src.[job_id]
           ,src.[job_name]
           ,src.[description]
           ,src.[job_category]
           ,src.[job_owner]
           ,src.[enabled]
           ,src.[notify_email_desc]
           ,src.[run_status_desc]
           ,src.[last_duration]
           ,src.[last_startdate]
		   ,0.0
           ,SYSUTCDATETIME() )

WHEN MATCHED THEN
UPDATE 
   SET [job_name] = src.[job_name]
      ,[description] = src.[description]
      ,[job_category] = src.[job_category]
      ,[job_owner] = src.[job_owner]
      ,[enabled] = src.[enabled]
      ,[notify_email_desc] = src.[notify_email_desc]
      ,[run_status_desc] = src.[run_status_desc]
      ,[last_duration] = src.[last_duration]
      ,[last_startdate] = src.[last_startdate]
      ,[LastUpdated] = SYSUTCDATETIME() 
;


END