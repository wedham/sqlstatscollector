/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [internal].[collectors_for_transfer]
   -----------------------------------------
   Returns collector names. Used for transferring data to central database

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-04-28	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [internal].[collectors_for_transfer]
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @serverid uniqueidentifier
	SELECT @serverid = [serverid]
	FROM [data].[server_properties]
	WHERE [MachineName] = CAST(SERVERPROPERTY('MachineName') AS nvarchar(128))

	SELECT [collector] = '[' + [collector] + ']'
	     , [serverid] = @serverid
    FROM [internal].[collectors]
	--WHERE [collector] NOT IN ('') --Filter on collectors that shouldn't be transferred
END