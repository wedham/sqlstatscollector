EXEC [collect].[availability_group_properties]
GO 1000 --00:00:01.6574435
EXEC [collect].[connection_properties]
GO 1000 --00:00:01.8050242
EXEC [collect].[cpu_stats]
GO 1000 --00:00:37.7664593
EXEC [collect].[database_cpu_usage]
WAITFOR DELAY '0:00:00.002'
GO 1000 --00:00:17.1129908
EXEC [collect].[database_memory_usage]
GO 1000 --00:01:04.1341229
EXEC [collect].[database_properties]
GO 1000 --00:00:51.0247185
EXEC [collect].[databasefile_properties]
GO 1000 --00:00:02.3469114
EXEC [collect].[databasefile_stats]
GO 1000 --00:00:29.6090478
EXEC [collect].[job_properties]
GO 1000 --00:00:00.7933721
EXEC [collect].[memory_stats]
GO 1000 --00:00:08.2778256
EXEC [collect].[server_properties]
GO 1000 --00:00:01.3450181
EXEC [collect].[server_stats]
WAITFOR DELAY '0:00:00.002'
GO 1000 --00:00:16.4885134
EXEC [collect].[wait_stats]
GO 1000 --00:00:06.2579283

