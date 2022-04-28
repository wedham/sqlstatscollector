CREATE TABLE [data].[cpu_stats] (
    [rowtime]     DATETIME2 (3) NOT NULL,
    [record_id]   INT           NOT NULL,
    [idle_cpu]    INT           NOT NULL,
    [sql_cpu]     INT           NOT NULL,
    [other_cpu]   INT           NOT NULL,
    [LastUpdated] DATETIME2 (7) NOT NULL,
    [LastHandled] DATETIME2 (7) NULL,
    CONSTRAINT [PK_cpu_stats] PRIMARY KEY CLUSTERED ([record_id] ASC)
);



