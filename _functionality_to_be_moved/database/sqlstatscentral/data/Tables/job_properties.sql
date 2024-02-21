CREATE TABLE [data].[job_properties] (
    [serverid]          UNIQUEIDENTIFIER NOT NULL,
    [job_id]            UNIQUEIDENTIFIER NOT NULL,
    [job_name]          NVARCHAR (128)   NOT NULL,
    [description]       NVARCHAR (512)   NOT NULL,
    [job_category]      NVARCHAR (128)   NOT NULL,
    [job_owner]         NVARCHAR (128)   NOT NULL,
    [enabled]           TINYINT          NOT NULL,
    [notify_email_desc] NVARCHAR (15)    NOT NULL,
    [run_status_desc]   NVARCHAR (15)    NOT NULL,
    [last_startdate]    DATETIME         NOT NULL,
    [last_duration]     DECIMAL (18, 3)  NOT NULL,
    [run_duration_avg]  DECIMAL (18, 3)  NOT NULL,
    [LastUpdated]       DATETIME2 (7)    NOT NULL,
    [LastHandled]       DATETIME2 (7)    NULL
);

