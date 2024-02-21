CREATE TABLE [data].[availability_group_properties] (
    [serverid]                    UNIQUEIDENTIFIER NOT NULL,
    [group_id]                    UNIQUEIDENTIFIER NOT NULL,
    [name]                        NVARCHAR (128)   NOT NULL,
    [primary_replica]             NVARCHAR (128)   NOT NULL,
    [recovery_health_desc]        NVARCHAR (60)    NULL,
    [synchronization_health_desc] NVARCHAR (60)    NULL,
    [LastUpdated]                 DATETIME2 (7)    NOT NULL,
    [LastHandled]                 DATETIME2 (7)    NULL,
    CONSTRAINT [PK_availability_group_properties] PRIMARY KEY CLUSTERED ([serverid] ASC, [group_id] ASC)
);

