CREATE PROCEDURE [internal].[initialize_collectors]
(@reset_cron bit = 0)
AS
BEGIN
	SET NOCOUNT ON;
	   --SELECT 'section', 'collectorname', 'cron expression' --Description
	WITH builtin_collector_list (section, collector, cron) AS
    (
	   SELECT 'core', 'server_properties', '0 6 * * *' --general server properties such as server name and version collected once a day
	   UNION ALL
	   SELECT 'core', 'database_properties', '0 6 * * *' --database properties and backup/dbcc info collected once a day
	   UNION ALL
	   SELECT 'core', 'wait_stats', '*/5 * * * *' --delta calculated wait stats collected every 5 minutes
	   UNION ALL
	   SELECT 'core', 'server_stats', '*/5 * * * *' --server/instance statistics collected every 5 minutes
	   UNION ALL
	   SELECT 'core', 'databasefile_properties', '0 */6 * * *' --database properties collected every 6 hours
	   UNION ALL
	   SELECT 'core', 'databasefile_stats', '*/5 * * * *' --delta calculated file sizes and IO stats stats collected every 5 minutes
	   UNION ALL
	   SELECT 'core', 'cpu_stats', '*/5 * * * *' --actual cpu consumtion stats collected every 5 minutes
	   UNION ALL
	   SELECT 'agent', 'job_properties', '0 * * * *' --Current jobs collected every hour
	   UNION ALL
	   SELECT 'agent', 'job_stats', '*/15 * * * *' --job durations collected every 15 minutes
	)
	MERGE [internal].[collectors] dest
	USING (SELECT section, collector, cron FROM builtin_collector_list) src
		ON src.collector = dest.collector
	WHEN MATCHED THEN
		UPDATE SET section = src.section
		, cron = CASE WHEN @reset_cron = 1 THEN src.cron ELSE dest.cron END
	WHEN NOT MATCHED THEN 
		INSERT (section, collector, cron)
		VALUES (src.section, src.collector, src.cron);

END