
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[availability_group_properties]
   -----------------------------------------
   Collects information about the server/instance.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-04	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   PROCEDURE [collect].[availability_group_properties]
AS
BEGIN
PRINT('[collect].[availability_group_properties] - Get properties of availability groups on the current server/instance')
SET NOCOUNT ON

	DECLARE @ag_info TABLE ([group_id] uniqueidentifier
	                      , [name] nvarchar(128)
						  , [primary_replica] nvarchar(128)
						  , [recovery_health_desc] nvarchar(60)
						  , [synchronization_health_desc] nvarchar(60)
						  ,	[LastUpdated] [datetime2](7) NOT NULL
)

	DECLARE @v varchar(20)

	SELECT @v = [internal].[GetSQLServerVersion]()

	IF (@v NOT IN ('2005', '2008', '2008R2'))
	BEGIN
		INSERT INTO @ag_info([group_id], [name], [primary_replica], [recovery_health_desc], [synchronization_health_desc], [LastUpdated])
		SELECT ag.group_id
			 , ag.name
		     , ags.primary_replica
			 , recovery_health_desc = ISNULL(ags.primary_recovery_health_desc, ags.secondary_recovery_health_desc)
			 , ags.synchronization_health_desc
			 , SYSUTCDATETIME()
		FROM sys.availability_groups ag INNER JOIN sys.dm_hadr_availability_group_states ags
		  ON ag.group_id = ags.group_id
	END;

	MERGE [data].[availability_group_properties] dest
	USING (	SELECT [group_id], [name], [primary_replica], [recovery_health_desc], [synchronization_health_desc], [LastUpdated] FROM @ag_info ) src
	  ON src.[group_id] = dest.[group_id]
    WHEN NOT MATCHED THEN
		INSERT ([group_id], [name], [primary_replica], [recovery_health_desc], [synchronization_health_desc], [LastUpdated])
		VALUES (src.[group_id], src.[name], src.[primary_replica], src.[recovery_health_desc], src.[synchronization_health_desc], src.[LastUpdated])
	WHEN MATCHED THEN
		UPDATE SET [name] = src.[name]
		         , [primary_replica] = src.[primary_replica]
				 , [recovery_health_desc] = src.[recovery_health_desc]
				 , [synchronization_health_desc] = src.[synchronization_health_desc]
				 , [LastUpdated] = src.[LastUpdated]
	;

END