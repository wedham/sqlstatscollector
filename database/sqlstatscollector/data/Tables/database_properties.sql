CREATE TABLE [data].[database_properties] (
    [database_id]             INT            NOT NULL,
    [name]                    NVARCHAR (128) NOT NULL,
    [owner_sid]               VARBINARY (85) NOT NULL,
    [create_date]             DATETIME       NOT NULL,
    [compatibility_level]     TINYINT        NOT NULL,
    [collation_name]          NVARCHAR (128) NULL,
    [is_auto_close_on]        BIT            NOT NULL,
    [is_auto_shrink_on]       BIT            NOT NULL,
    [state_desc]              NVARCHAR (60)  NOT NULL,
    [recovery_model_desc]     NVARCHAR (60)  NOT NULL,
    [page_verify_option_desc] NVARCHAR (60)  NOT NULL,
    [LastFullBackupTime]      DATETIME       NULL,
    [LastDiffBackupTime]      DATETIME       NULL,
    [LastLogBackupTime]       DATETIME       NULL,
    [LastKnownGoodDBCCTime]   DATETIME       NULL,
    [LastUpdated]             DATETIME2 (7)  NOT NULL,
    [LastHandled]             DATETIME2 (7)  NULL,
    CONSTRAINT [PK_database_properties] PRIMARY KEY CLUSTERED ([database_id] ASC)
);



