CREATE TABLE [data].[server_stats] (
    [serverid]           UNIQUEIDENTIFIER NOT NULL,
    [user_connections]   INT              NOT NULL,
    [batch_requests_sec] INT              NOT NULL,
    [rowtime]            DATETIME2 (7)    NOT NULL,
    [LastUpdated]        DATETIME2 (7)    NOT NULL,
    [LastHandled]        DATETIME2 (7)    NULL
);

