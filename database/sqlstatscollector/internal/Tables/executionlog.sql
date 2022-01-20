CREATE TABLE [internal].[executionlog] (
    [Id]          INT           IDENTITY (1, 1) NOT NULL,
    [collector]   VARCHAR (100) NOT NULL,
    [StartTime]   DATETIME2 (3) NOT NULL,
    [EndTime]     DATETIME2 (3) NULL,
    [Duration_ms] BIGINT        NULL,
    [errornumber] INT           NULL,
    CONSTRAINT [PK_executionlog] PRIMARY KEY CLUSTERED ([Id] ASC)
);

