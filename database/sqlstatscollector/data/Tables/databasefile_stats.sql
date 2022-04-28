CREATE TABLE [data].[databasefile_stats] (
    [rowtime]              DATETIME2 (7)   NOT NULL,
    [database_id]          INT             NOT NULL,
    [file_id]              INT             NOT NULL,
    [size_mb]              DECIMAL (19, 4) NOT NULL,
    [freespace_mb]         DECIMAL (19, 4) NOT NULL,
    [num_of_reads]         BIGINT          NOT NULL,
    [num_of_bytes_read]    BIGINT          NOT NULL,
    [io_stall_read_ms]     BIGINT          NOT NULL,
    [num_of_writes]        BIGINT          NOT NULL,
    [num_of_bytes_written] BIGINT          NOT NULL,
    [io_stall_write_ms]    BIGINT          NOT NULL,
    [LastUpdated]          DATETIME2 (7)   NOT NULL,
    [LastHandled]          DATETIME2 (7)   NULL
);



