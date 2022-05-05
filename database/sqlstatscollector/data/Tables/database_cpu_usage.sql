CREATE TABLE [data].[database_cpu_usage] (
    [rowtime]     DATETIME2 (7)   NOT NULL,
    [database_id] INT             NOT NULL,
    [cpu_time_ms] DECIMAL (18, 3) NOT NULL,
    [cpu_percent] DECIMAL (5, 2)  NOT NULL,
    [LastUpdated] DATETIME2 (7)   NOT NULL,
    [LastHandled] DATETIME2 (7)   NULL
);

