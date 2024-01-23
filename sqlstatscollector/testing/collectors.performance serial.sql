USE [sqlstatscollector]
GO

EXEC [collect].[availability_group_properties]
EXEC [collect].[connection_properties]
EXEC [collect].[cpu_stats]
EXEC [collect].[database_cpu_usage]
EXEC [collect].[database_memory_usage]
EXEC [collect].[database_properties]
EXEC [collect].[databasefile_properties]
EXEC [collect].[databasefile_stats]
EXEC [collect].[job_properties]
EXEC [collect].[memory_stats]
EXEC [collect].[server_properties]
EXEC [collect].[server_stats]
EXEC [collect].[wait_stats]
GO 1000000
