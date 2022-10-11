CREATE TABLE [data].[connection_properties] (
    [db_name]          NVARCHAR (128) NOT NULL,
    [host_name]        NVARCHAR (128) NOT NULL,
    [login_name]       NVARCHAR (128) NOT NULL,
    [program_name]     NVARCHAR (128) NOT NULL,
    [connection_count] BIGINT         NOT NULL,
    [last_seen]        DATETIME2 (0)  NOT NULL
);

