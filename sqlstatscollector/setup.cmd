@echo off
cd /d %~dp0

SET sqlserver=.
SET database=sqlstatscollector

sqlcmd -d master -E -C -S %sqlserver% -v DatabaseName = "%database%" -i _initialize.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i cron.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.base.functionality.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.server_properties.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.server_stats.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.memory_stats.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.cpu_stats.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.wait_stats.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.database_properties.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.databasefile_properties.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.databasefile_stats.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.database_memory_usage.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.database_cpu_usage.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.connection_properties.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.job_properties.sql 
sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.collector.availability_group_properties.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.run.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatscollector.status_of_collectors.sql 

sqlcmd -d msdb -E -C -S %sqlserver% -i sqlstatscollector.scheduling.sql 

pause