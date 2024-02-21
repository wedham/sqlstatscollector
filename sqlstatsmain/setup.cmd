@echo off
cd /d %~dp0

SET sqlserver=.
SET database=sqlstatsmain

sqlcmd -d master -E -C -S %sqlserver% -v DatabaseName = "%database%" -i _initialize.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i cron.sql 

sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.base.functionality.sql 

REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.server_properties.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.server_stats.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.memory_stats.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.cpu_stats.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.wait_stats.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.database_properties.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.databasefile_properties.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.databasefile_stats.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.database_memory_usage.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.database_cpu_usage.sql 

REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.connection_properties.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.job_properties.sql 
REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.collector.availability_group_properties.sql 

REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.run.sql 

REM sqlcmd -d %database% -E -C -S %sqlserver% -i sqlstatsmain.status_of_collectors.sql 

REM sqlcmd -d msdb -E -C -S %sqlserver% -i sqlstatsmain.scheduling.sql 

pause