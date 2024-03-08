[CmdletBinding()]
param(
    # SQLServer defaults to '.' can take an array of sqlservers
    [Parameter(Mandatory = $false)]
    [String[]]$SQLServer = '.'
)

[String]$DatabaseName = 'sqlstatsmain'


sqlcmd -d master -E -C -S $SQLServer -v DatabaseName = "$DatabaseName" -i _initialize.sql 

sqlcmd -d $DatabaseName -E -C -S $SQLServer -i cron.sql 

sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.base.functionality.sql 

# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.server_properties.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.server_stats.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.memory_stats.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.cpu_stats.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.wait_stats.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.database_properties.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.databasefile_properties.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.databasefile_stats.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.database_memory_usage.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.database_cpu_usage.sql 




# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.availability_group_properties.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.connection_properties.sql 
# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.collector.job_properties.sql

# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.run.sql 

# sqlcmd -d $DatabaseName -E -C -S $SQLServer -i sqlstatsmain.status_of_collectors.sql 

# sqlcmd -d msdb -E -C -S $SQLServer -i sqlstatsmain.scheduling.sql 
