CREATE TABLE [data].[memory_stats] (
    [rowtime]                      DATETIME2 (7)   NOT NULL,
    [page_life_expectancy]         INT             NOT NULL,
    [target_server_memory_mb]      BIGINT          NOT NULL,
    [total_server_memory_mb]       BIGINT          NOT NULL,
    [total_physical_memory_mb]     BIGINT          NOT NULL,
    [available_physical_memory_mb] BIGINT          NOT NULL,
    [percent_memory_used]          DECIMAL (18, 3) NOT NULL,
    [system_memory_state_desc]     NVARCHAR (256)  NOT NULL,
    [LastUpdated]                  DATETIME2 (7)   NOT NULL,
    [LastHandled]                  DATETIME2 (7)   NULL
);



