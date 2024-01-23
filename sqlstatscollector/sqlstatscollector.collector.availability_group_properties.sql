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
DECLARE @TableDefinitionHash varbinary(32) = 0x2E501F7E4FEA796EA596DFC30142A48B501C18F92E089FC70E8A80C36287E954

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
	RAISERROR(N'DROP original table', 10, 1) WITH NOWAIT
	SELECT @cmd = N'DROP TABLE ' + @FullName
	EXEC (@cmd)
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	SELECT @msg = N'Creating ' + @FullName
	RAISERROR(@msg, 10, 1) WITH NOWAIT
	CREATE TABLE [data].[availability_group_properties](
		[group_id] [uniqueidentifier] NOT NULL,
		[name] [nvarchar](128) NOT NULL,
		[primary_replica] [nvarchar](128) NOT NULL,
		[recovery_health_desc] [nvarchar](60) NULL,
		[synchronization_health_desc] [nvarchar](60) NULL,
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_availability_group_properties] PRIMARY KEY CLUSTERED 
			(
				[group_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END
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
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
	VALUES (N'availability_group_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @ag_info TABLE ([group_id] uniqueidentifier
						, [name] nvarchar(128)
						, [primary_replica] nvarchar(128)
						, [recovery_health_desc] nvarchar(60)
						, [synchronization_health_desc] nvarchar(60)
						,	[LastUpdated] [datetime2](7) NOT NULL
							)

	DECLARE @v varchar(20)	
	
	BEGIN TRY

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

	END TRY
	BEGIN CATCH
		DECLARE @msg nvarchar(4000)
		SELECT @error = ERROR_NUMBER(), @msg = ERROR_MESSAGE()
		PRINT (@msg)
	END CATCH

	SELECT @current_end = SYSUTCDATETIME()
	UPDATE [internal].[executionlog]
	SET [EndTime] = @current_end
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
	SET [LastHandled] = SYSUTCDATETIME()
	OUTPUT @serverid serverid 
	     , inserted.[group_id]
		 , inserted.[name]
		 , inserted.[primary_replica]
		 , inserted.[recovery_health_desc]
		 , inserted.[synchronization_health_desc]
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[availability_group_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

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
