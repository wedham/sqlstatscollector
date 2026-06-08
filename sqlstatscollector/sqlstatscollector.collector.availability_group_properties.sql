SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [availability_group_properties]', 10, 1) WITH NOWAIT
GO

----------------------------------------------------------------
-- Table [data].[availability_group_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[availability_group_properties] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'availability_group_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x8D5884943CBD2C8E894C929F919704613C1B2F65118948B66C014D177E7C411C

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
		[is_local] [bit] NULL,
		[is_primary_replica] [bit] NULL,
		[primary_replica_name] [nvarchar](128) NOT NULL,
		[is_suspended] [bit] NULL,
		[synchronization_state_desc] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[is_failover_ready] [bit] NOT NULL,
		[redo_queue_size] [bigint] NULL,
		[redo_rate] [bigint] NULL,
		[last_commit_time] [datetime] NULL,
		[EstimatedRTO] [nvarchar](30) NOT NULL,
		[EstimatedRPO] [nvarchar](30) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_availability_group_properties] PRIMARY KEY CLUSTERED 
			(
				[group_database_id] ASC,
				[replica_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT

GO


----------------------------------------------------------------
-- Table [data].[availability_group_properties_changes]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[availability_group_properties_changes] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'availability_group_properties_changes'
DECLARE @TableDefinitionHash varbinary(32) = 0x2FFE10FD77D2F1CEB124989D53FCA3200E15F9FA39054382FC4AA5F17C9AA2C2

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
	CREATE TABLE [data].[availability_group_properties_changes](
		[rowtimeutc] [datetime2](7) NOT NULL,
		[group_database_id] [uniqueidentifier] NOT NULL,
		[replica_id] [uniqueidentifier] NOT NULL,
		[propertyname] [nvarchar](128) NOT NULL,
		[old_value] [nvarchar](256) NOT NULL,
		[new_value] [nvarchar](256) NOT NULL,
	) ON [PRIMARY]
END

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


----------------------------------------------------------------
-- Trigger [data].[availability_group_properties_change]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Trigger [data].[availability_group_properties_change] ******/', 10, 1) WITH NOWAIT
GO
CREATE OR ALTER TRIGGER [data].[availability_group_properties_change]
ON [data].[availability_group_properties]
AFTER UPDATE
AS
BEGIN

    INSERT INTO [data].[availability_group_properties_changes] ([rowtimeutc], [group_database_id], [replica_id], [propertyname], [old_value], [new_value])
    SELECT i.[LastUpdatedUTC], i.[group_database_id], i.[replica_id], changedata.propertyname, changedata.old_value, changedata.new_value
    FROM inserted i INNER JOIN deleted d ON i.[group_database_id] = d.[group_database_id] AND i.[replica_id] = d.[replica_id]
    CROSS APPLY ( VALUES 
    -- Insert a list of columns for change tracking here.

                  (N'[database_id]'                  , CAST(d.[database_id] AS nvarchar(256))                , CAST(i.[database_id] AS nvarchar(256)))
                 ,(N'[group_id]'                     , CAST(d.[group_id] AS nvarchar(256))                 , CAST(i.[group_id] AS nvarchar(256)))
                 ,(N'[availability_group_name]'      , CAST(d.[availability_group_name] AS nvarchar(256))                   , CAST(i.[availability_group_name] AS nvarchar(256)))
                 ,(N'[is_local]'                     , CAST(d.[is_local] AS nvarchar(256)), CAST(i.[is_local] AS nvarchar(256)))
                 ,(N'[is_primary_replica]'           , CAST(d.[is_primary_replica] AS nvarchar(256))                    , CAST(i.[is_primary_replica] AS nvarchar(256)))
                 ,(N'[primary_replica_name]'         , CAST(d.[primary_replica_name] AS nvarchar(256))               , CAST(i.[primary_replica_name] AS nvarchar(256)))
                 ,(N'[is_suspended]'                 , CAST(d.[is_suspended] AS nvarchar(256))             , CAST(i.[is_suspended] AS nvarchar(256)))
                 ,(N'[synchronization_state_desc]'   , CAST(d.[synchronization_state_desc] AS nvarchar(256))                , CAST(i.[synchronization_state_desc] AS nvarchar(256)))
                 ,(N'[synchronization_health_desc]'  , CAST(d.[synchronization_health_desc] AS nvarchar(256))  , CAST(i.[synchronization_health_desc] AS nvarchar(256)))
                 ,(N'[is_failover_ready]'            , CAST(d.[is_failover_ready] AS nvarchar(256))              , CAST(i.[is_failover_ready] AS nvarchar(256)))
                 ,(N'[redo_queue_size]'              , CAST(d.[redo_queue_size] AS nvarchar(256))         , CAST(i.[redo_queue_size] AS nvarchar(256)))
                 ,(N'[redo_rate]'                    , CAST(d.[redo_rate] AS nvarchar(256))                  , CAST(i.[redo_rate] AS nvarchar(256)))
                 ,(N'[last_commit_time]'             , CONVERT(nvarchar(256), d.[last_commit_time], 121)               , CONVERT(nvarchar(256), i.[last_commit_time], 121))
                 ,(N'[EstimatedRTO]'                 , CAST(d.[EstimatedRTO] AS nvarchar(256))           , CAST(i.[EstimatedRTO] AS nvarchar(256)))
                 ,(N'[EstimatedRPO]'                 , CAST(d.[EstimatedRPO] AS nvarchar(256))  , CAST(i.[EstimatedRPO] AS nvarchar(256)))

    --End of column list             
    ) changedata (propertyname ,old_value ,new_value)
    WHERE changedata.old_value <> changedata.new_value


END
GO


GO



----------------------------------------------------------------
-- StoredProcedure [collect].[availability_group_properties]
----------------------------------------------------------------
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
2026-06-02	Mikael Wedham		History functionality added
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
		[is_local] [bit] NULL,
		[is_primary_replica] [bit] NULL,
		[primary_replica_name] [nvarchar](128) NOT NULL,
		[is_suspended] [bit] NULL,
		[synchronization_state_desc] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[is_failover_ready] [bit] NOT NULL,
		[redo_queue_size] [bigint] NULL,
		[redo_rate] [bigint] NULL,
		[last_commit_time] [datetime] NULL,
		[EstimatedRTO] [nvarchar](30) NOT NULL,
		[EstimatedRPO] [nvarchar](30) NOT NULL,
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
			), cte_availability_group_properties AS
			(
				SELECT drs.database_id
					, drs.replica_id
					, drs.group_id
					, drs.group_database_id
					, [availability_group_name] = ag.[name]
					, drs.is_local
					, drs.is_primary_replica
					, [primary_replica_name] = ags.primary_replica
					, drs.is_suspended
					, drs.synchronization_state_desc
					, drs.synchronization_health_desc
					, drcs.is_failover_ready
					, drs.redo_queue_size
					, drs.redo_rate
					, drs.last_commit_time
				
					, EstimatedRTO  = CASE WHEN drs.is_primary_replica = 1   THEN N'Primary Replica'
										   WHEN drs.redo_queue_size = 0      THEN N'Nothing to redo'
										   WHEN ISNULL(drs.redo_rate, 0) = 0 THEN N'No estimation available'
                                           ELSE CAST( CAST(( ( (1000*drs.redo_queue_size) / drs.redo_rate ) / 1000.0) AS decimal(18,3))  AS nvarchar(20))  + N' s'
									  END

					, EstimatedRPO = CASE WHEN drs.is_local = 0 THEN N'Remote replica'
										  WHEN drs.last_commit_time IS NULL
											OR drcs.is_failover_ready IS NULL
											OR primary_last_commit_time IS NULL THEN N'RPO calculation unavailable'
										  WHEN drcs.is_failover_ready = 1       THEN N'Failover ready, no dataloss'
										  ELSE CONVERT (nvarchar, DATEADD(ms, DATEDIFF(ss, drs.last_commit_time, primary_last_commit_time) * 1000, 0), 114)
									 END
					, [LastUpdatedUTC] = SYSUTCDATETIME()
				FROM sys.availability_groups ag 
					INNER JOIN sys.dm_hadr_availability_group_states ags 
							ON ag.group_id = ags.group_id
					INNER JOIN sys.dm_hadr_database_replica_states drs 
							ON drs.group_id = ag.group_id
					LEFT OUTER JOIN sys.dm_hadr_database_replica_cluster_states drcs 
							ON drs.replica_id = drcs.replica_id 
							AND drs.group_database_id = drcs.group_database_id
					LEFT OUTER JOIN primary_replicas pr 
							ON drs.group_id = pr.group_id 
							AND drs.group_database_id = pr.group_database_id
							)
MERGE [data].[availability_group_properties] dest USING
(SELECT * 
 FROM cte_availability_group_properties) src
 ON src.[group_database_id] = dest.[group_database_id] AND src.[replica_id] = dest.[replica_id]

		WHEN NOT MATCHED THEN 
			INSERT ([database_id] ,[replica_id] ,[group_id] ,[group_database_id] ,[availability_group_name] 
			       ,[is_local] ,[is_primary_replica] ,[primary_replica_name]
			       ,[is_suspended] ,[synchronization_state_desc] ,[synchronization_health_desc] ,[is_failover_ready]
				   ,[redo_queue_size] ,[redo_rate] ,[last_commit_time] ,[EstimatedRTO] ,[EstimatedRPO] ,[LastUpdatedUTC])
			VALUES (src.[database_id] ,src.[replica_id] ,src.[group_id] ,src.[group_database_id] ,src.[availability_group_name]
			       ,src.[is_local] ,src.[is_primary_replica] ,src.[primary_replica_name]
			       ,src.[is_suspended] ,src.[synchronization_state_desc] ,src.[synchronization_health_desc] ,src.[is_failover_ready]
				   ,src.[redo_queue_size] ,src.[redo_rate] ,src.[last_commit_time] ,src.[EstimatedRTO] ,src.[EstimatedRPO] ,src.[LastUpdatedUTC] )
		WHEN MATCHED THEN
			UPDATE SET 
		   [database_id] = src.[database_id]
		   ,[group_id] = src.[group_id]
		   ,[availability_group_name] = src.[availability_group_name]
		   ,[is_local] = src.[is_local]
		   ,[is_primary_replica] = src.[is_primary_replica]
		   ,[primary_replica_name] = src.[primary_replica_name]
		   ,[is_suspended] = src.[is_suspended]
		   ,[synchronization_state_desc] = src.[synchronization_state_desc]
		   ,[synchronization_health_desc] = src.[synchronization_health_desc]
		   ,[is_failover_ready] = src.[is_failover_ready]
		   ,[redo_queue_size] = src.[redo_queue_size]
		   ,[redo_rate] = src.[redo_rate]
		   ,[last_commit_time] = src.[last_commit_time]
		   ,[EstimatedRTO] = src.[EstimatedRTO]
		   ,[EstimatedRPO] = src.[EstimatedRPO]
		   ,[LastUpdatedUTC] = src.[LastUpdatedUTC]
			;

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

----------------------------------------------------------------
-- StoredProcedure [transfer].[availability_group_properties]
----------------------------------------------------------------
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

	DECLARE @ag_info TABLE (
		[serverid] [uniqueidentifier] NOT NULL,
		[database_id] [int] NOT NULL,
		[replica_id] [uniqueidentifier] NOT NULL,
		[group_id] [uniqueidentifier] NOT NULL,
		[group_database_id] [uniqueidentifier] NOT NULL,
		[availability_group_name] [nvarchar](128) NOT NULL,
		[is_local] [bit] NULL,
		[is_primary_replica] [bit] NULL,
		[primary_replica_name] [nvarchar](128) NOT NULL,
		[is_suspended] [bit] NULL,
		[synchronization_state_desc] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[is_failover_ready] [bit] NOT NULL,
		[redo_queue_size] [bigint] NULL,
		[redo_rate] [bigint] NULL,
		[last_commit_time] [datetime] NULL,
		[EstimatedRTO] [nvarchar](30) NOT NULL,
		[EstimatedRPO] [nvarchar](30) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL
					)

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
      ,inserted.[synchronization_state_desc]
      ,inserted.[synchronization_health_desc]
      ,inserted.[is_failover_ready]
      ,inserted.[redo_queue_size]
      ,inserted.[redo_rate]
      ,inserted.[last_commit_time]
      ,inserted.[EstimatedRTO]
      ,inserted.[EstimatedRPO]
      ,inserted.[LastUpdatedUTC]
      ,inserted.[LastHandledUTC]
  INTO @ag_info
  FROM [data].[availability_group_properties] s
  WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]

	SELECT ai.[serverid]
		,ai.[database_id]
		,ai.[replica_id]
		,ai.[group_id]
		,ai.[group_database_id]
		,ai.[availability_group_name]
		,ai.[is_local]
		,ai.[is_primary_replica]
		,ai.[primary_replica_name]
		,ai.[is_suspended]
		,ai.[synchronization_state_desc]
		,ai.[synchronization_health_desc]
		,ai.[is_failover_ready]
		,ai.[redo_queue_size]
		,ai.[redo_rate]
		,ai.[last_commit_time]
		,ai.[EstimatedRTO]
		,ai.[EstimatedRPO]
		,ai.[LastUpdatedUTC]
		,ai.[LastHandledUTC]
	FROM @ag_info ai

END
GO


----------------------------------------------------------------
-- Finalizing [availability_group_properties]
----------------------------------------------------------------
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
