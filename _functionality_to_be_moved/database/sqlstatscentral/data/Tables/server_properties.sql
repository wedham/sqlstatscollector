CREATE TABLE [data].[server_properties] (
    [serverid]                    UNIQUEIDENTIFIER NOT NULL,
    [MachineName]                 NVARCHAR (128)   NOT NULL,
    [ServerName]                  NVARCHAR (128)   NOT NULL,
    [Instance]                    NVARCHAR (128)   NULL,
    [ComputerNamePhysicalNetBIOS] NVARCHAR (128)   NULL,
    [Edition]                     NVARCHAR (128)   NOT NULL,
    [ProductLevel]                NVARCHAR (128)   NOT NULL,
    [ProductVersion]              NVARCHAR (128)   NOT NULL,
    [Collation]                   NVARCHAR (128)   NOT NULL,
    [IsClustered]                 INT              NULL,
    [IsIntegratedSecurityOnly]    INT              NULL,
    [FilestreamConfiguredLevel]   INT              NULL,
    [IsHadrEnabled]               INT              NULL,
    [LastUpdated]                 DATETIME2 (7)    NOT NULL,
    [LastHandled]                 DATETIME2 (7)    NULL
);

