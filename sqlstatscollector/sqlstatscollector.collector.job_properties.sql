SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [job_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'job_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xA497C157E24F569965B7A1DD58D31C8CE175A041D0256E1436970B8AA90CD36D

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
		[LastUpdated] [datetime2](7) NOT NULL,
		[LastHandled] [datetime2](7) NULL,
		 CONSTRAINT [PK_job_properties] PRIMARY KEY CLUSTERED 
			(
				[job_id] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END
GO




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
	INSERT INTO [internal].[executionlog] ([collector], [StartTime])
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
				,[LastUpdated])
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
			,[LastUpdated] = SYSUTCDATETIME() 
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
*******************************************************************************/
ALTER PROCEDURE [transfer].[job_properties]
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
		 , inserted.[LastUpdated]
		 , inserted.[LastHandled]
	FROM [data].[job_properties] s
	WHERE [LastHandled] IS NULL OR [LastUpdated] > [LastHandled]

END
GO

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
