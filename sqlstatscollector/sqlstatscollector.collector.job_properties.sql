SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [job_properties]', 10, 1) WITH NOWAIT
GO

----------------------------------------------------------------
-- Table [data].[job_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[job_properties] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'job_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x649721BBC7292D233699F117823C7FBEACE0347FC2793FEED6A8F367663DF415

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
	CREATE TABLE [data].[job_properties](
		[job_id] [uniqueidentifier] NOT NULL,
		[job_name] [nvarchar](128) NOT NULL,
		[description] [nvarchar](512) NOT NULL,
		[job_category] [nvarchar](128) NOT NULL,
		[job_owner] [nvarchar](128) NOT NULL,
		[enabled] [tinyint] NOT NULL,
		[notify_email_desc] [nvarchar](15) NOT NULL,
		[run_status_desc] [nvarchar](15) NOT NULL,
		[last_startdate] [datetime] NOT NULL,
		[last_duration] [decimal](18, 3) NOT NULL,
		[run_duration_avg] [decimal](18, 3) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_job_properties] PRIMARY KEY CLUSTERED 
			(
				[job_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT @msg = N'Table ' + [FullName] + ' was found with checksum ' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

----------------------------------------------------------------
-- Table [data].[job_properties_changes]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Table [data].[job_properties_changes] ******/', 10, 1) WITH NOWAIT

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'job_properties_changes'
DECLARE @TableDefinitionHash varbinary(32) = 0x2DBDD2D88F38076AB89C321ECA5FA9EFB509FD117CD12B70442B8D6A6B8557A2

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
	CREATE TABLE [data].[job_properties_changes](
		[rowtimeutc] [datetime2](7) NOT NULL,
		[job_id] [uniqueidentifier] NOT NULL,
		[propertyname] [nvarchar](128) NOT NULL,
		[old_value] [nvarchar](256) NOT NULL,
		[new_value] [nvarchar](256) NOT NULL,
	) ON [PRIMARY]
END

SELECT @msg = N'Table ' + [FullName] + ' was found with checksum ' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO


----------------------------------------------------------------
-- Trigger [data].[job_properties_change]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  Trigger [data].[job_properties_change] ******/', 10, 1) WITH NOWAIT
GO
CREATE OR ALTER TRIGGER [data].[job_properties_change]
ON [data].[job_properties]
AFTER UPDATE
AS
BEGIN

    INSERT INTO [data].[job_properties_changes] ([rowtimeutc], [job_id], [propertyname], [old_value], [new_value])
    SELECT i.[LastUpdatedUTC], i.[job_id], changedata.propertyname, changedata.old_value, changedata.new_value
    FROM inserted i INNER JOIN deleted d ON i.[job_id] = d.[job_id] 
    CROSS APPLY ( VALUES 
    -- Insert a list of columns for change tracking here.

                  (N'[job_name]'           , CAST(d.[job_name] AS nvarchar(256))             , CAST(i.[job_name] AS nvarchar(256)))
                 --,(N'[description]'        , CAST(d.[description] AS nvarchar(256))          , CAST(i.[description] AS nvarchar(256)))
                 --,(N'[job_category]'       , CAST(d.[job_category] AS nvarchar(256))         , CAST(i.[job_category] AS nvarchar(256)))
                 ,(N'[job_owner]'          , CAST(d.[job_owner] AS nvarchar(256))            , CAST(i.[job_owner] AS nvarchar(256)))
                 ,(N'[enabled]'            , CAST(d.[enabled] AS nvarchar(256))              , CAST(i.[enabled] AS nvarchar(256)))
                 ,(N'[notify_email_desc]'  , CAST(d.[notify_email_desc] AS nvarchar(256))    , CAST(i.[notify_email_desc] AS nvarchar(256)))
                 --,(N'[run_status_desc]'    , CAST(d.[run_status_desc] AS nvarchar(256))      , CAST(i.[run_status_desc] AS nvarchar(256)))
                 --,(N'[last_startdate]'     , CONVERT(nvarchar(256), d.[last_startdate], 121) , CONVERT(nvarchar(256), i.[last_startdate], 121))
                 --,(N'[last_duration]'      , CAST(d.[last_duration] AS nvarchar(256))        , CAST(i.[last_duration] AS nvarchar(256)))
                 --,(N'[run_duration_avg]'   , CAST(d.[run_duration_avg] AS nvarchar(256))     , CAST(i.[run_duration_avg] AS nvarchar(256)))

    --End of column list             
    ) changedata (propertyname ,old_value ,new_value)
    WHERE changedata.old_value <> changedata.new_value

END
GO


----------------------------------------------------------------
-- StoredProcedure [collect].[job_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  StoredProcedure [collect].[job_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[job_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[job_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[job_properties]
   -----------------------------------------
   Get all defined jobs in the server

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-21	Mikael Wedham		+Created v1
2022-04-27	Mikael Wedham		+Brackets and formatting
2024-01-17  Mikael Wedham		+Refactored for use with synonyms in msdb
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-06-03	Mikael Wedham		History functionality added
*******************************************************************************/
ALTER PROCEDURE [collect].[job_properties]
AS
BEGIN
PRINT('[collect].[job_properties] - Get all defined jobs in the server')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'job_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	BEGIN TRY

		;
		WITH [details] AS (
					SELECT [rowselector] = ROW_NUMBER() OVER (PARTITION BY h.[job_id] ORDER BY h.[run_date] DESC, h.[run_time] DESC)
					, h.[job_id]
					, h.[run_duration]
					FROM [dbo].[sysjobhistory] h
					WHERE h.[step_id] = 0
					), [jobdurations] AS
					(
					SELECT [job_id]
					, [run_duration_avg] = CAST(AVG([run_duration] * 1.0) AS decimal(18,3))
					FROM [details]
					WHERE [rowselector] <= 50
					GROUP BY [job_id]
					)
		MERGE [data].[job_properties] dest
		USING (
		SELECT job_id = sj.[job_id]
			, job_name = sj.[name]
			, [description] = sj.[description] 
			, job_category = sc.[name]
			, job_owner = ISNULL(SUSER_SNAME(sj.[owner_sid]), CAST(sj.[owner_sid] as nvarchar(255)))
			, [enabled] = sj.[enabled]
			, notify_email_desc = CASE WHEN [notify_email_operator_id] = 0 OR [notify_level_email] = 0 THEN 'NONE'
									WHEN [notify_level_email] = 1 THEN 'SUCCESS'
									WHEN [notify_level_email] = 2 THEN 'FAILURE'
									WHEN [notify_level_email] = 3 THEN 'COMPLETION'
									END
			, run_status_desc = CASE h.[run_status] WHEN 0 THEN 'FAILED'
													WHEN 1 THEN 'SUCCEEDED'
													WHEN 2 THEN 'RETRY'
													WHEN 3 THEN 'CANCELLED'
													WHEN 4 THEN 'IN PROGRESS'
													END
			, last_startdate = CONVERT(DATETIME, RTRIM(h.[run_date]) + ' ' + STUFF(STUFF(REPLACE(STR(RTRIM(h.[run_time]),6,0),' ','0'),3,0,':'),6,0,':'))
			, last_duration = h.[run_duration]
			, run_duration_avg = d.[run_duration_avg]
		FROM [dbo].[sysjobs] AS sj WITH (NOLOCK)
		INNER JOIN
			(SELECT [job_id], instance_id = MAX([instance_id])
			FROM [dbo].[sysjobhistory] WITH (NOLOCK)
			GROUP BY [job_id]) AS l
		ON sj.[job_id] = l.[job_id]
		INNER JOIN [dbo].[syscategories] AS sc WITH (NOLOCK)
		ON sj.[category_id] = sc.[category_id]
		LEFT OUTER JOIN [dbo].[sysjobhistory] AS h WITH (NOLOCK)
		ON h.[job_id] = l.[job_id]
		AND h.[instance_id] = l.[instance_id]
		LEFT OUTER JOIN [jobdurations] d
		ON d.[job_id] = sj.[job_id]
		) src
		ON src.[job_id] = dest.[job_id]
		WHEN NOT MATCHED THEN
		INSERT     ([job_id]
				,[job_name]
				,[description]
				,[job_category]
				,[job_owner]
				,[enabled]
				,[notify_email_desc]
				,[run_status_desc]
				,[last_duration]
				,[last_startdate]
				,[run_duration_avg]
				,[LastUpdatedUTC])
			VALUES
				(src.[job_id]
				,src.[job_name]
				,src.[description]
				,src.[job_category]
				,src.[job_owner]
				,src.[enabled]
				,src.[notify_email_desc]
				,src.[run_status_desc]
				,src.[last_duration]
				,src.[last_startdate]
				,0.0
				,SYSUTCDATETIME() )

		WHEN MATCHED THEN
		UPDATE 
		SET [job_name] = src.[job_name]
			,[description] = src.[description]
			,[job_category] = src.[job_category]
			,[job_owner] = src.[job_owner]
			,[enabled] = src.[enabled]
			,[notify_email_desc] = src.[notify_email_desc]
			,[run_status_desc] = src.[run_status_desc]
			,[last_duration] = src.[last_duration]
			,[last_startdate] = src.[last_startdate]
			,[run_duration_avg] = src.[run_duration_avg]
			,[LastUpdatedUTC] = SYSUTCDATETIME() 
		;

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
-- StoredProcedure [transfer].[job_properties]
----------------------------------------------------------------
RAISERROR(N'/****** Object:  StoredProcedure [transfer].[job_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[job_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[job_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[job_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-06-03	Mikael Wedham		History functionality added
*******************************************************************************/
ALTER PROCEDURE [transfer].[job_properties]
AS
BEGIN
	SET NOCOUNT ON
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	DECLARE @job_properties TABLE (
		[serverid] [uniqueidentifier] NOT NULL,
		[job_id] [uniqueidentifier] NOT NULL,
		[job_name] [nvarchar](128) NOT NULL,
		[description] [nvarchar](512) NOT NULL,
		[job_category] [nvarchar](128) NOT NULL,
		[job_owner] [nvarchar](128) NOT NULL,
		[enabled] [tinyint] NOT NULL,
		[notify_email_desc] [nvarchar](15) NOT NULL,
		[run_status_desc] [nvarchar](15) NOT NULL,
		[last_startdate] [datetime] NOT NULL,
		[last_duration] [decimal](18, 3) NOT NULL,
		[run_duration_avg] [decimal](18, 3) NOT NULL,
		[LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL)



	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
	OUTPUT @serverid serverid 
	     , inserted.[job_id]
		 , inserted.[job_name]
		 , inserted.[description]
		 , inserted.[job_category]
		 , inserted.[job_owner]
		 , inserted.[enabled]
		 , inserted.[notify_email_desc]
		 , inserted.[run_status_desc]
		 , inserted.[last_startdate]
		 , inserted.[last_duration]
		 , inserted.[run_duration_avg]
		 , inserted.[LastUpdatedUTC]
		 , inserted.[LastHandledUTC]
	INTO @job_properties
	FROM [data].[job_properties] s
	WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]

	SELECT jp.serverid 
	     , jp.[job_id]
		 , jp.[job_name]
		 , jp.[description]
		 , jp.[job_category]
		 , jp.[job_owner]
		 , jp.[enabled]
		 , jp.[notify_email_desc]
		 , jp.[run_status_desc]
		 , jp.[last_startdate]
		 , jp.[last_duration]
		 , jp.[run_duration_avg]
		 , jp.[LastUpdatedUTC]
		 , jp.[LastHandledUTC]
	FROM @job_properties jp

END
GO

----------------------------------------------------------------
-- Finalizing [job_properties]
----------------------------------------------------------------
RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'agent', 'job_properties', '0 * * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
