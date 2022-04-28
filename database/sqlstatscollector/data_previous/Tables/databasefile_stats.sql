CREATE TABLE [data_previous].[databasefile_stats] (
    [database_id]          INT    NOT NULL,
    [file_id]              INT    NOT NULL,
    [size]                 INT    NOT NULL,
    [free_pages]           INT    NOT NULL,
    [num_of_reads]         BIGINT NOT NULL,
    [num_of_bytes_read]    BIGINT NOT NULL,
    [io_stall_read_ms]     BIGINT NOT NULL,
    [num_of_writes]        BIGINT NOT NULL,
    [num_of_bytes_written] BIGINT NOT NULL,
    [io_stall_write_ms]    BIGINT NOT NULL
);

