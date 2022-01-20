CREATE TABLE [internal].[collectors] (
    [section]   VARCHAR (100) NOT NULL,
    [collector] VARCHAR (100) NOT NULL,
    [cron]      VARCHAR (255) NOT NULL,
    [lastrun]   DATETIME2 (0) CONSTRAINT [DF_collectors_lastrun] DEFAULT ('2000-01-01') NOT NULL,
    CONSTRAINT [PK_Config] PRIMARY KEY CLUSTERED ([collector] ASC)
);

