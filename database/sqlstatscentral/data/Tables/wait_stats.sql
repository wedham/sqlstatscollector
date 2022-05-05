CREATE TABLE [data].[wait_stats] (
    [serverid]                   UNIQUEIDENTIFIER NOT NULL,
    [rowtime]                    DATETIME2 (7)    NOT NULL,
    [wait_type]                  NVARCHAR (127)   NOT NULL,
    [interval_percentage]        DECIMAL (18, 3)  NOT NULL,
    [wait_time_seconds]          DECIMAL (18, 3)  NOT NULL,
    [resource_wait_time_seconds] DECIMAL (18, 3)  NOT NULL,
    [signal_wait_time_seconds]   DECIMAL (18, 3)  NOT NULL,
    [wait_count]                 BIGINT           NOT NULL,
    [LastUpdated]                DATETIME2 (7)    NOT NULL,
    [LastHandled]                DATETIME2 (7)    NULL
);

