SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

RAISERROR(N'Collector: [server_properties]', 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'incoming'
DECLARE @TableName nvarchar(128) = N'server_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0x6B37F22708814CCA37D1D7C613D775A8ADC2F17062B91B4A80556BA1EAABA9CF

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
	CREATE TABLE [incoming].[server_properties](
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
        [physical_memory_mb] [decimal](18,3) NOT NULL,
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

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO

DECLARE @SchemaName nvarchar(128) = N'data'
DECLARE @TableName nvarchar(128) = N'server_properties'
DECLARE @TableDefinitionHash varbinary(32) = 0xBABC1DA598676B8EB21E3FA65904D51F553A44757CDB912DBE3179E09B0356CE

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
		[physical_memory_mb] [decimal](18,3) NOT NULL,
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

SELECT @msg = N'Table:' + [FullName] + ' Checksum:' + CONVERT(nvarchar(100), [TableDefinitionHash], 1)
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

RAISERROR(@msg, 10, 1) WITH NOWAIT
GO



RAISERROR(N'/****** Object:  StoredProcedure [transfer].[server_properties] ******/', 10, 1) WITH NOWAIT
GO

IF OBJECT_ID(N'[transfer].[server_properties]', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [transfer].[server_properties] AS SELECT NULL')
END
GO


/*******************************************************************************
   --Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [transfer].[server_properties]
   -----------------------------------------
   Merges data from [incoming] to [data].

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-02-21	Mikael Wedham		+Created v1
2026-02-04	Marcus Petö			+Added MERGE function
2026-06-08	Mikael Wedham		Adapted datatypes and column names to history v1
*******************************************************************************/
ALTER PROCEDURE [transfer].[server_properties]
(
	@serverid [uniqueidentifier]
)
AS
BEGIN
	SET NOCOUNT ON

	MERGE [data].[server_properties] dest
	USING
	(
		SELECT
			 [serverid]
			,[MachineName]
			,[ServerName]
			,[Instance]
			,[ComputerNamePhysicalNetBIOS]
			,[Edition]
			,[ProductLevel]
			,[ProductVersion]
			,[Collation]
			,[IsClustered]
			,[IsIntegratedSecurityOnly]
			,[FilestreamConfiguredLevel]
			,[IsHadrEnabled]
			,[physical_memory_mb]
            ,[cpu_count]
            ,[socket_count]
            ,[cores_per_socket]
            ,[virtual_machine_type_desc]		
            ,[sqlserver_start_time]
			,[LastUpdatedUTC]
			,[LastHandledUTC]
		FROM [incoming].[server_properties]
		WHERE	[serverid] = @serverid
	) src
	ON src.[serverid] = dest.[serverid]
	WHEN NOT MATCHED THEN
		INSERT 
			(
				 [serverid]
				,[MachineName]
				,[ServerName]
				,[Instance]
				,[ComputerNamePhysicalNetBIOS]
				,[Edition]
				,[ProductLevel]
				,[ProductVersion]
				,[Collation]
				,[IsClustered]
				,[IsIntegratedSecurityOnly]
				,[FilestreamConfiguredLevel]
				,[IsHadrEnabled]
			    ,[physical_memory_mb]
                ,[cpu_count]
                ,[socket_count]
                ,[cores_per_socket]
                ,[virtual_machine_type_desc]		
                ,[sqlserver_start_time]
				,[LastUpdatedUTC]
				,[LastHandledUTC]
			)
			VALUES
			(
				 src.[serverid]
				,src.[MachineName]
				,src.[ServerName]
				,src.[Instance]
				,src.[ComputerNamePhysicalNetBIOS]
				,src.[Edition]
				,src.[ProductLevel]
				,src.[ProductVersion]
				,src.[Collation]
				,src.[IsClustered]
				,src.[IsIntegratedSecurityOnly]
				,src.[FilestreamConfiguredLevel]
				,src.[IsHadrEnabled]
				,src.[physical_memory_mb]
                ,src.[cpu_count]
                ,src.[socket_count]
                ,src.[cores_per_socket]
                ,src.[virtual_machine_type_desc]		
                ,src.[sqlserver_start_time]
			    ,src.[LastUpdatedUTC]
				,src.[LastHandledUTC]
			)
	WHEN MATCHED AND src.[LastUpdatedUTC] <> dest.[LastUpdatedUTC] THEN
		UPDATE SET
					 dest.[serverid] = src.[serverid]
					,dest.[MachineName] = src.[MachineName]
					,dest.[ServerName] = src.[ServerName]
					,dest.[Instance] = src.[Instance]
					,dest.[ComputerNamePhysicalNetBIOS] = src.[ComputerNamePhysicalNetBIOS]
					,dest.[Edition] = src.[Edition]
					,dest.[ProductLevel] = src.[ProductLevel]
					,dest.[ProductVersion] = src.[ProductVersion]
					,dest.[Collation] = src.[Collation]
					,dest.[IsClustered] = src.[IsClustered]
					,dest.[IsIntegratedSecurityOnly] = src.[IsIntegratedSecurityOnly]
					,dest.[FilestreamConfiguredLevel] = src.[FilestreamConfiguredLevel]
					,dest.[IsHadrEnabled] = src.[IsHadrEnabled]
					,dest.[physical_memory_mb] = src.[physical_memory_mb]
					,dest.[cpu_count] = src.[cpu_count]
					,dest.[socket_count] = src.[socket_count]
					,dest.[cores_per_socket] = src.[cores_per_socket]
					,dest.[virtual_machine_type_desc] = src.[virtual_machine_type_desc]
					,dest.[sqlserver_start_time] = src.[sqlserver_start_time]
					,dest.[LastUpdatedUTC] = src.[LastUpdatedUTC]
					,dest.[LastHandledUTC] = src.[LastHandledUTC]
			;

	DELETE FROM [incoming].[server_properties]
	WHERE [serverid] = @serverid
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
