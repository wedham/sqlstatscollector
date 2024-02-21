CREATE TABLE [internal].[sqlserverinstances] (
    [serverkey]        BIGINT           IDENTITY (1, 1) NOT NULL,
    [serverid]         UNIQUEIDENTIFIER NULL,
    [InstanceName]     NVARCHAR (255)   NOT NULL,
    [UserName]         NVARCHAR (50)    NULL,
    [Password]         NVARCHAR (50)    NULL,
    [LastConnection]   DATETIME2 (7)    NULL,
    [ConnectionString] NVARCHAR (1000)  NULL,
    CONSTRAINT [PK_sqlserverinstances] PRIMARY KEY CLUSTERED ([serverkey] ASC)
);

