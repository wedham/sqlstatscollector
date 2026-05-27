SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [availability_group_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'availability_group_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x91F35DC30BD4C4D5B4B891EB27C075ADBF0466AD00E71DD87431B87BDCFEBBD7

DECLARE @TableExists int
DECLARE @TableHasChanged int
DECLARE @FullName nvarchar(255)
DECLARE @NewName nvarchar(128)

DECLARE @cmd nvarchar(2048)
DECLARE @msg nvarchar(2048)

SELECT @FullName = [FullName]
     , @TableExists = [TableExists]
     , @TableHasChanged = [TableHasChanged]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

IF @TableExists = 1 AND @TableHasChanged = 1
BEGIN
	SELECT @cmd = N'DROP TABLE ' + @FullName
	RAISERROR(@cmd, 10, 1) WITH NOWAIT
	EXEC (@cmd)
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	SELECT @msg = N'Creating ' + @FullName
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	CREATE TABLE [data].[availability_group_properties](
		[database_id] [int] NOT NULL,
		[replica_id] [uniqueidentifier] NOT NULL,
		[group_id] [uniqueidentifier] NOT NULL,
		[group_database_id] [uniqueidentifier] NOT NULL,
		[availability_group_name] [nvarchar](128) NOT NULL,
		[is_local] [bit] NOT NULL,
		[is_primary_replica] [bit] NOT NULL,
		[primary_replica_name] [nvarchar](128) NOT NULL,
		[is_suspended] [bit] NOT NULL,
		[synchronization_state] [nvarchar](60) NULL,
		[synchronization_state_desc] [nvarchar](60) NULL,
		[synchronization_health] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[is_failover_ready] [bit] NOT NULL,
		[redo_queue_size] [bigint] NOT NULL,
		[redo_rate] [bigint] NOT NULL,
		[last_commit_time] [datetime] NOT NULL,
		[EstimatedRecoveryTimeObjective] [nvarchar](20) NOT NULL,
		[EstimatedRecoveryPointObjective] [nvarchar](20) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_availability_group_properties] PRIMARY KEY CLUSTERED 
			(
				[replica_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

RAISERROR(N'/****** Object:  StoredProcedure [collect].[availability_group_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[availability_group_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[availability_group_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[availability_group_properties]
   -----------------------------------------
   Collects information about the server/instance.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-04	Mikael Wedham		+Created v1
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-04-01	Mikael Wedham		Added replica state information
2026-05-25	Mikael Wedham		Changed collection to all rows 
*******************************************************************************/
ALTER PROCEDURE [collect].[availability_group_properties]
AS
BEGIN
PRINT('[collect].[availability_group_properties] - Get properties of availability groups on the current server/instance')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'availability_group_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @ag_info TABLE ([database_id] [int] NOT NULL,
		[replica_id] [uniqueidentifier] NOT NULL,
		[group_id] [uniqueidentifier] NOT NULL,
		[group_database_id] [uniqueidentifier] NOT NULL,
		[availability_group_name] [nvarchar](128) NOT NULL,
		[is_local] [bit] NOT NULL,
		[is_primary_replica] [bit] NOT NULL,
		[primary_replica_name] [nvarchar](128) NOT NULL,
		[is_suspended] [bit] NOT NULL,
		[synchronization_state] [nvarchar](60) NULL,
		[synchronization_state_desc] [nvarchar](60) NULL,
		[synchronization_health] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[is_failover_ready] [bit] NOT NULL,
		[redo_queue_size] [bigint] NOT NULL,
		[redo_rate] [bigint] NOT NULL,
		[last_commit_time] [datetime] NOT NULL,
		[EstimatedRecoveryTimeObjective] [nvarchar](20) NOT NULL,
		[EstimatedRecoveryPointObjective] [nvarchar](20) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL
							)

	DECLARE @v varchar(20)	
	
	BEGIN TRY

		SELECT @v = [internal].[GetSQLServerVersion]()

		IF (@v NOT IN ('2005', '2008', '2008R2'))
		BEGIN
		;WITH primary_replicas AS
			(
				SELECT primary_last_commit_time = dbr.last_commit_time
					 , dbr.group_id
					 , dbr.group_database_id
				FROM sys.dm_hadr_database_replica_states AS dbr
				WHERE dbr.is_primary_replica = 1
			)
			INSERT INTO [data].[availability_group_properties]
			                   ([database_id] ,[replica_id] ,[group_id] ,[group_database_id] ,[availability_group_name] ,[is_local] ,[is_primary_replica] ,[primary_replica_name]
			                   ,[is_suspended] ,[synchronization_state] ,[synchronization_state_desc] ,[synchronization_health] ,[synchronization_health_desc] ,[is_failover_ready]
							   ,[redo_queue_size] ,[redo_rate] ,[last_commit_time] ,[EstimatedRecoveryTimeObjective] ,[EstimatedRecoveryPointObjective] ,[LastUpdatedUTC])

SELECT drs.database_id
     , drs.replica_id
     , drs.group_id
     , drs.group_database_id
     , ag.[name]
     , drs.is_local
     , drs.is_primary_replica
	 , ags.primary_replica
     , drs.is_suspended
     , drs.synchronization_state, drs.synchronization_state_desc
     , drs.synchronization_health, drs.synchronization_health_desc
     , drcs.is_failover_ready
     , drs.redo_queue_size
     , drs.redo_rate
     , drs.last_commit_time

, EstimatedRecoveryTimeObjective = CASE WHEN drs.is_primary_replica = 1   THEN 'Primary Replica'
                                        WHEN drs.redo_queue_size = 0      THEN 'Nothing to redo'
										WHEN ISNULL(drs.redo_rate, 0) = 0 THEN 'No estimation available'
                                                                          ELSE CAST( CAST(( ( (1000*drs.redo_queue_size) / drs.redo_rate ) / 1000.0) AS decimal(18,3))  AS nvarchar(20))  + ' s'
																		  END

, EstimatedRecoveryPointObjective = CASE WHEN drs.is_local = 0 THEN 'Remote replica'
                                         WHEN drs.last_commit_time IS NULL
                                           OR drcs.is_failover_ready IS NULL
                                           OR primary_last_commit_time IS NULL THEN 'RPO calculation unavailable'
                                         WHEN drcs.is_failover_ready = 1       THEN 'Failover ready, no dataloss'
										                                       ELSE CONVERT (nvarchar, DATEADD(ms, DATEDIFF(ss, drs.last_commit_time, primary_last_commit_time) * 1000, 0), 114)
																		       END
, SYSUTCDATETIME()

      
FROM sys.availability_groups ag INNER JOIN sys.dm_hadr_availability_group_states ags ON ag.group_id = ags.group_id
 INNER JOIN sys.dm_hadr_database_replica_states AS drs ON drs.group_id = ag.group_id
    LEFT OUTER JOIN sys.dm_hadr_database_replica_cluster_states AS drcs ON drs.replica_id = drcs.replica_id AND drs.group_database_id = drcs.group_database_id
    LEFT OUTER JOIN primary_replicas pr ON drs.group_id = pr.group_id AND drs.group_database_id = pr.group_database_id

		END;

	END TRY
	BEGIN CATCH
		DECLARE @msg nvarchar(4000)
		SELECT @error = ERROR_NUMBER(), @msg = ERROR_MESSAGE()
		PRINT (@msg)
	END CATCH

	SELECT @current_end = SYSUTCDATETIME()
	UPDATE [internal].[executionlog]
	SET [EndTimeUTC] = @current_end
	, [Duration_ms] =  ((CAST(DATEDIFF(S, @current_start, @current_end) AS bigint) * 1000000) + (DATEPART(MCS, @current_end)-DATEPART(MCS, @current_start))) / 1000.0
	, [errornumber] = @@ERROR
	WHERE [Id] = @current_logitem

END
GO


RAISERROR(N'/****** Object:  StoredProcedure [transfer].[availability_group_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[availability_group_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[availability_group_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[availability_group_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-05-04	Mikael Wedham		+Created v1
2026-03-31	Mikael Wedham		Adding UTC to column names
*******************************************************************************/
ALTER PROCEDURE [transfer].[availability_group_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
      OUTPUT @serverid serverid
	  ,inserted.[database_id]
      ,inserted.[replica_id]
      ,inserted.[group_id]
      ,inserted.[group_database_id]
      ,inserted.[availability_group_name]
      ,inserted.[is_local]
      ,inserted.[is_primary_replica]
      ,inserted.[primary_replica_name]
      ,inserted.[is_suspended]
      ,inserted.[synchronization_state]
      ,inserted.[synchronization_state_desc]
      ,inserted.[synchronization_health]
      ,inserted.[synchronization_health_desc]
      ,inserted.[is_failover_ready]
      ,inserted.[redo_queue_size]
      ,inserted.[redo_rate]
      ,inserted.[last_commit_time]
      ,inserted.[EstimatedRecoveryTimeObjective]
      ,inserted.[EstimatedRecoveryPointObjective]
      ,inserted.[LastUpdatedUTC]
      ,inserted.[LastHandledUTC]
  FROM [data].[availability_group_properties] s
  WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]

END
GO



RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'hadr', 'availability_group_properties', '*/20 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
