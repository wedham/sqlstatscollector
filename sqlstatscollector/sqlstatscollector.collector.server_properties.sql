SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [server_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'server_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xB797EBC471DB2A4353146799C495503ACFC94FECCC843AF8F6AD1EA55DFFB2B4

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
	CREATE TABLE [data].[server_properties](
		[serverid] [uniqueidentifier] NOT NULL,
		[MachineName] [nvarchar](128) NOT NULL,
		[ServerName] [nvarchar](128) NOT NULL,
		[Instance] [nvarchar](128) NULL,
		[ComputerNamePhysicalNetBIOS] [nvarchar](128) NULL,
		[Edition] [nvarchar](128) NOT NULL,
		[ProductLevel] [nvarchar](128) NOT NULL,
		[ProductVersion] [nvarchar](128) NOT NULL,
		[Collation] [nvarchar](128) NOT NULL,
		[IsClustered] [int] NULL,
		[IsIntegratedSecurityOnly] [int] NULL,
		[FilestreamConfiguredLevel] [int] NULL,
		[IsHadrEnabled] [int] NULL,
        [physical_memory_kb] [bigint] NOT NULL,
        [cpu_count] [int] NOT NULL,
        [socket_count] [int] NOT NULL,
        [cores_per_socket] [int] NOT NULL,
        [virtual_machine_type_desc] [nvarchar](60) NOT NULL,		
        [sqlserver_start_time] [datetime] NOT NULL,
        [LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL,
		 CONSTRAINT [PK_data_server_properties] PRIMARY KEY CLUSTERED 
			(
				[serverid] ASC
			) ON [PRIMARY]
		) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)


SET @TableName = N'server_properties_changes'
SET @TableDefinitionHash = 0xC40A0C0394EF22B0DD10854A0669CB2D60AB4BE5A07ABF84DD45F5FBE92107BD


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
	CREATE TABLE [data].[server_properties_changes](
		[rowtimeutc] [datetime2](7) NOT NULL,
		[serverid] [uniqueidentifier] NOT NULL,
		[propertyname] [nvarchar](128) NOT NULL,
		[old_value] [nvarchar](256) NOT NULL,
		[new_value] [nvarchar](256) NOT NULL,
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO


RAISERROR(N'/****** Object:  Trigger [data].[server_properties_change] ******/', 10, 1) WITH NOWAIT
GO
CREATE OR ALTER TRIGGER [data].[server_properties_change]
ON [data].[server_properties]
AFTER UPDATE
AS
BEGIN

    INSERT INTO [data].[server_properties_changes] ([rowtimeutc], [serverid], [propertyname], [old_value], [new_value])
    SELECT i.[LastUpdatedUTC], i.[serverid], changedata.propertyname, changedata.old_value, changedata.new_value
    FROM inserted i INNER JOIN deleted d ON i.[serverid] = d.[serverid] 
    CROSS APPLY ( VALUES 
    -- Insert a list of columns for change tracking here.
                  (N'[MachineName]'                , CAST(d.[MachineName] AS nvarchar(256))                , CAST(i.[MachineName] AS nvarchar(256)))
                 ,(N'[ServerName]'                 , CAST(d.[ServerName] AS nvarchar(256))                 , CAST(i.[ServerName] AS nvarchar(256)))
                 ,(N'[Instance]'                   , CAST(d.[Instance] AS nvarchar(256))                   , CAST(i.[Instance] AS nvarchar(256)))
                 ,(N'[ComputerNamePhysicalNetBIOS]', CAST(d.[ComputerNamePhysicalNetBIOS] AS nvarchar(256)), CAST(i.[ComputerNamePhysicalNetBIOS] AS nvarchar(256)))
                 ,(N'[Edition]'                    , CAST(d.[Edition] AS nvarchar(256))                    , CAST(i.[Edition] AS nvarchar(256)))
                 ,(N'[ProductLevel]'               , CAST(d.[ProductLevel] AS nvarchar(256))               , CAST(i.[ProductLevel] AS nvarchar(256)))
                 ,(N'[ProductVersion]'             , CAST(d.[ProductVersion] AS nvarchar(256))             , CAST(i.[ProductVersion] AS nvarchar(256)))
                 ,(N'[Collation]'                  , CAST(d.[Collation] AS nvarchar(256))                  , CAST(i.[Collation] AS nvarchar(256)))
                 ,(N'[IsClustered]'                , CAST(d.[IsClustered] AS nvarchar(256))                , CAST(i.[IsClustered] AS nvarchar(256)))
                 ,(N'[IsIntegratedSecurityOnly]'   , CAST(d.[IsIntegratedSecurityOnly] AS nvarchar(256))   , CAST(i.[IsIntegratedSecurityOnly] AS nvarchar(256)))
                 ,(N'[FilestreamConfiguredLevel]'  , CAST(d.[FilestreamConfiguredLevel] AS nvarchar(256))  , CAST(i.[FilestreamConfiguredLevel] AS nvarchar(256)))
                 ,(N'[IsHadrEnabled]'              , CAST(d.[IsHadrEnabled] AS nvarchar(256))              , CAST(i.[IsHadrEnabled] AS nvarchar(256)))
                 ,(N'[physical_memory_kb]'         , CAST(d.[physical_memory_kb] AS nvarchar(256))         , CAST(i.[physical_memory_kb] AS nvarchar(256)))
                 ,(N'[cpu_count]'                  , CAST(d.[cpu_count] AS nvarchar(256))                  , CAST(i.[cpu_count] AS nvarchar(256)))
                 ,(N'socket_count'                 , CAST(d.[socket_count] AS nvarchar(256))               , CAST(i.[socket_count] AS nvarchar(256)))
                 ,(N'[cores_per_socket]'           , CAST(d.[cores_per_socket] AS nvarchar(256))           , CAST(i.[cores_per_socket] AS nvarchar(256)))
                 ,(N'[virtual_machine_type_desc]'  , CAST(d.[virtual_machine_type_desc] AS nvarchar(256))  , CAST(i.[virtual_machine_type_desc] AS nvarchar(256)))
                 ,(N'[sqlserver_start_time]'       , CONVERT(nvarchar(256), d.[sqlserver_start_time], 121) , CONVERT(nvarchar(256), i.[sqlserver_start_time], 121))
    --End of column list             
    ) changedata (propertyname ,old_value ,new_value)
    WHERE changedata.old_value <> changedata.new_value


END
GO





RAISERROR(N'/****** Object:  StoredProcedure [collect].[server_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[collect].[server_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [collect].[server_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [collect].[server_properties]
   -----------------------------------------
   Collects information about the server/instance.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-21	Mikael Wedham		+Created v1
2024-01-17  Mikael Wedham		+Added value generation for serverid
2024-01-19	Mikael Wedham		+Added logging of duration
2024-01-23	Mikael Wedham		+Added errorhandling
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-05-27	Mikael Wedham		Cores and memory added #29
*******************************************************************************/
ALTER PROCEDURE [collect].[server_properties]
AS
BEGIN
PRINT('[collect].[server_properties] - Get all properties of the current server//instance')
SET NOCOUNT ON

	DECLARE @current_start datetime2(7)
	DECLARE @current_end datetime2(7)
	DECLARE @current_logitem int
	DECLARE @error int = 0

	SELECT @current_start = SYSUTCDATETIME()
	INSERT INTO [internal].[executionlog] ([collector], [StartTimeUTC])
	VALUES (N'server_properties', @current_start)
	SET @current_logitem = SCOPE_IDENTITY()

	DECLARE @FilestreamConfiguredLevel int = null
	DECLARE @IsHadrEnabled int = null

	DECLARE @v varchar(20)

	BEGIN TRY

		SELECT @v = [internal].[GetSQLServerVersion]()

		IF (@v NOT IN ('2005', '2008', '2008R2'))
		BEGIN
		SELECT @FilestreamConfiguredLevel = CAST(SERVERPROPERTY('FilestreamConfiguredLevel') AS int)
			, @IsHadrEnabled = CAST(SERVERPROPERTY('IsHadrEnabled') AS int)
		END;

		MERGE  [data].[server_properties] dest
		USING ( SELECT [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))
					, [ServerName] = CAST(SERVERPROPERTY('ServerName') AS nvarchar(128))
					, [Instance] = CAST(SERVERPROPERTY('InstanceName') AS nvarchar(128))
					, [ComputerNamePhysicalNetBIOS] = CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS nvarchar(128))
					, [Edition] = CAST(SERVERPROPERTY('Edition') AS nvarchar(128))
					, [ProductLevel] = CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128))
					, [ProductVersion] = CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128))
					, [Collation] = CAST(SERVERPROPERTY('Collation') AS nvarchar(128))
					, [IsClustered] = CAST(SERVERPROPERTY('IsClustered') AS int)
					, [IsIntegratedSecurityOnly] = CAST(SERVERPROPERTY('IsIntegratedSecurityOnly') AS int)
					, [FilestreamConfiguredLevel] = @FilestreamConfiguredLevel
					, [IsHadrEnabled] = @IsHadrEnabled
                    , [physical_memory_kb]
                    , [cpu_count]
                    , [socket_count]
                    , [cores_per_socket]
                    , [virtual_machine_type_desc]
					, [sqlserver_start_time]
                    FROM [sys].[dm_os_sys_info]
					) src
		ON dest.[MachineName] = src.[MachineName]	
		WHEN NOT MATCHED THEN
			INSERT ([serverid]
				, [MachineName]
				, [ServerName]
				, [Instance]
				, [ComputerNamePhysicalNetBIOS]
				, [Edition]
				, [ProductLevel]
				, [ProductVersion]
				, [Collation]
				, [IsClustered]
				, [IsIntegratedSecurityOnly]
				, [FilestreamConfiguredLevel]
				, [IsHadrEnabled]
				, [physical_memory_kb]
				, [cpu_count]
				, [socket_count]
				, [cores_per_socket]
				, [virtual_machine_type_desc]
				, [sqlserver_start_time]
				, [LastUpdatedUTC] )
			VALUES
				( NEWID()
				, [MachineName]
				, [ServerName]
				, [Instance]
				, [ComputerNamePhysicalNetBIOS]
				, [Edition]
				, [ProductLevel]
				, [ProductVersion]
				, [Collation]
				, [IsClustered]
				, [IsIntegratedSecurityOnly]
				, [FilestreamConfiguredLevel]
				, [IsHadrEnabled]
				, [physical_memory_kb]
				, [cpu_count]
				, [socket_count]
				, [cores_per_socket]
				, [virtual_machine_type_desc]
				, [sqlserver_start_time]
				, SYSUTCDATETIME())
		WHEN MATCHED THEN
			UPDATE SET 
			  [ServerName] = src.[ServerName]
			, [Instance] = src.[Instance]
			, [ComputerNamePhysicalNetBIOS] = src.[ComputerNamePhysicalNetBIOS]
			, [Edition] = src.[Edition]
			, [ProductLevel] = src.[ProductLevel]
			, [ProductVersion] = src.[ProductVersion]
			, [Collation] = src.[Collation]
			, [IsClustered] = src.[IsClustered]
			, [IsIntegratedSecurityOnly] = src.[IsIntegratedSecurityOnly]
			, [FilestreamConfiguredLevel] = src.[FilestreamConfiguredLevel]
			, [IsHadrEnabled] = src.[IsHadrEnabled]
			, [physical_memory_kb] = src.[physical_memory_kb]
			, [cpu_count] = src.[cpu_count]
			, [socket_count] = src.[socket_count]
			, [cores_per_socket] = src.[cores_per_socket]
			, [virtual_machine_type_desc] = src.[virtual_machine_type_desc]
			, [sqlserver_start_time] = src.[sqlserver_start_time]
			,[LastUpdatedUTC] = SYSUTCDATETIME();

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



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[server_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[server_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[server_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[server_properties]
   -----------------------------------------
   Prepares and marks collected data as transferred. Returns the rows that
   are updated since last transfer.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
2026-03-31	Mikael Wedham		Adding UTC to column names
2026-05-27	Mikael Wedham		Cores and memory added
*******************************************************************************/
ALTER PROCEDURE [transfer].[server_properties]
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @server_properties TABLE (
		[serverid] [uniqueidentifier] NOT NULL,
		[MachineName] [nvarchar](128) NOT NULL,
		[ServerName] [nvarchar](128) NOT NULL,
		[Instance] [nvarchar](128) NULL,
		[ComputerNamePhysicalNetBIOS] [nvarchar](128) NULL,
		[Edition] [nvarchar](128) NOT NULL,
		[ProductLevel] [nvarchar](128) NOT NULL,
		[ProductVersion] [nvarchar](128) NOT NULL,
		[Collation] [nvarchar](128) NOT NULL,
		[IsClustered] [int] NULL,
		[IsIntegratedSecurityOnly] [int] NULL,
		[FilestreamConfiguredLevel] [int] NULL,
		[IsHadrEnabled] [int] NULL,
        [physical_memory_kb] [bigint] NOT NULL,
        [cpu_count] [int] NOT NULL,
        [socket_count] [int] NOT NULL,
        [cores_per_socket] [int] NOT NULL,
        [virtual_machine_type_desc] [nvarchar](60) NOT NULL,		
        [sqlserver_start_time] [datetime] NOT NULL,
        [LastUpdatedUTC] [datetime2](7) NOT NULL,
		[LastHandledUTC] [datetime2](7) NULL)


	UPDATE s
	SET [LastHandledUTC] = SYSUTCDATETIME()
	OUTPUT inserted.[serverid] 
	     , inserted.[MachineName]
		 , inserted.[ServerName]
		 , inserted.[Instance]
		 , inserted.[ComputerNamePhysicalNetBIOS]
		 , inserted.[Edition]
		 , inserted.[ProductLevel]
		 , inserted.[ProductVersion]
		 , inserted.[Collation]
		 , inserted.[IsClustered]
		 , inserted.[IsIntegratedSecurityOnly]
		 , inserted.[FilestreamConfiguredLevel]
		 , inserted.[IsHadrEnabled]
         , inserted.[physical_memory_kb] 
         , inserted.[cpu_count] 
         , inserted.[socket_count] 
         , inserted.[cores_per_socket] 
         , inserted.[virtual_machine_type_desc]		
         , inserted.[sqlserver_start_time]
		 , inserted.[LastUpdatedUTC]
		 , inserted.[LastHandledUTC]
	INTO @server_properties
	FROM [data].[server_properties] s
	WHERE [LastHandledUTC] IS NULL OR [LastUpdatedUTC] > [LastHandledUTC]
	AND [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

		SELECT sp.[serverid] 
	         , sp.[MachineName]
		     , sp.[ServerName]
		     , sp.[Instance]
		     , sp.[ComputerNamePhysicalNetBIOS]
		     , sp.[Edition]
		     , sp.[ProductLevel]
		     , sp.[ProductVersion]
		     , sp.[Collation]
		     , sp.[IsClustered]
		     , sp.[IsIntegratedSecurityOnly]
		     , sp.[FilestreamConfiguredLevel]
		     , sp.[IsHadrEnabled]
             , sp.[physical_memory_kb] 
             , sp.[cpu_count] 
             , sp.[socket_count] 
             , sp.[cores_per_socket] 
             , sp.[virtual_machine_type_desc]		
             , sp.[sqlserver_start_time]
		     , sp.[LastUpdatedUTC]
		     , sp.[LastHandledUTC]
		 FROM @server_properties sp

END
GO

RAISERROR(N'Adding collector to [internal].[collectors]', 10, 1) WITH NOWAIT
GO

WITH collector ([section], [collector], [cron]) AS
(SELECT 'core', 'server_properties', '0 6 * * *')
MERGE [internal].[collectors] dest
	USING (SELECT [section], [collector], [cron] FROM [collector]) src
		ON src.[collector] = dest.[collector]
	WHEN NOT MATCHED THEN 
		INSERT ([section], [collector], [cron], [lastrun], [is_enabled])
		VALUES (src.[section], src.[collector], src.[cron], '2000-01-01', 1);
GO
