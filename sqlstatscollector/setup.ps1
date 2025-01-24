[CmdletBinding()]
param(
    # SQLServer defaults to '.' can take an array of sqlservers
    [Parameter(Mandatory = $false)]
    [String[]]$SQLServer = '.'
)

[String]$DatabaseName = 'sqlstatscollector'

foreach ($SQL in $SQLServer) {
    Write-Host "Initializing $DatabaseName on $SQL" -ForegroundColor Green
    sqlcmd -d master -E -C -S $SQL -v DatabaseName = "$DatabaseName" -i _initialize.sql 

    Write-Host 'Configuring solution' -ForegroundColor DarkGreen
    sqlcmd -d $DatabaseName -E -C -S $SQL -i cron.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.base.functionality.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.server_properties.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.server_stats.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.memory_stats.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.cpu_stats.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.wait_stats.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.database_properties.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.databasefile_properties.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.databasefile_stats.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.database_memory_usage.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.database_cpu_usage.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.connection_properties.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.job_properties.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.collector.availability_group_properties.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.run.sql 
    sqlcmd -d $DatabaseName -E -C -S $SQL -i sqlstatscollector.status_of_collectors.sql
    
    Write-Host 'Configure sqlstatscollector scheduling' -ForegroundColor DarkGreen
    sqlcmd -d msdb -E -C -S $SQL -i sqlstatscollector.scheduling.sql 

    Write-Host "Configuration done on server $SQL" -ForegroundColor DarkCyan
}