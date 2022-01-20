CREATE FUNCTION [internal].[GetSQLServerVersion]()
RETURNS varchar(20)
WITH SCHEMABINDING
AS
BEGIN

  DECLARE @result varchar(20)

  DECLARE @version varchar(128)

  SELECT @version = CONVERT(varchar(128), SERVERPROPERTY('ProductVersion'))

  SELECT @result =
         CASE WHEN @version LIKE '9%'     THEN '2005'
              WHEN @version LIKE '10%'    THEN '2008'
              WHEN @version LIKE '10.5%'  THEN '2008R2'
              WHEN @version LIKE '11%'    THEN '2012'
              WHEN @version LIKE '12%'    THEN '2014'
              WHEN @version LIKE '13%'    THEN '2016'
              WHEN @version LIKE '14%'    THEN '2017'
              WHEN @version LIKE '15%'    THEN '2019'
   ELSE 'Unknown' END

  RETURN (@result)

END

