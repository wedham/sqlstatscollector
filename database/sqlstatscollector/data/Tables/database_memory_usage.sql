CREATE TABLE [data].[database_memory_usage] (
    [rowtime]             DATETIME2 (7)   NOT NULL,
    [database_id]         INT             NOT NULL,
    [page_count]          INT             NOT NULL,
    [cached_size_mb]      DECIMAL (15, 2) NOT NULL,
    [buffer_pool_percent] DECIMAL (5, 2)  NOT NULL,
    [LastUpdated]         DATETIME2 (7)   NOT NULL,
    [LastHandled]         DATETIME2 (7)   NULL
);

