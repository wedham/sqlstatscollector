CREATE TABLE [data_previous].[wait_stats] (
    [wait_type]                  NVARCHAR (127)  NOT NULL,
    [wait_time_seconds]          DECIMAL (18, 3) NOT NULL,
    [resource_wait_time_seconds] DECIMAL (18, 3) NOT NULL,
    [signal_wait_time_seconds]   DECIMAL (18, 3) NOT NULL,
    [wait_count]                 BIGINT          NOT NULL
);

