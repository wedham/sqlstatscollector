CREATE TABLE [data].[databasefile_properties] (
    [serverid]       UNIQUEIDENTIFIER NOT NULL,
    [database_id]    INT              NOT NULL,
    [file_id]        INT              NOT NULL,
    [type_desc]      NVARCHAR (60)    NOT NULL,
    [name]           NVARCHAR (128)   NOT NULL,
    [physical_name]  NVARCHAR (260)   NOT NULL,
    [state_desc]     NVARCHAR (60)    NOT NULL,
    [size_mb]        DECIMAL (19, 4)  NOT NULL,
    [max_size_mb]    INT              NULL,
    [growth_mb]      INT              NULL,
    [growth_percent] INT              NULL,
    [LastUpdated]    DATETIME2 (7)    NOT NULL,
    [LastHandled]    DATETIME2 (7)    NULL
);

