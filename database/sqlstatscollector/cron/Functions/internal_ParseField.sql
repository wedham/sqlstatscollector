
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[internal_ParseField]
   -----------------------------------------
   Returns a list of numbers based on a cron expression field
   A field is the base of the CRON expression
   This function is a wrapper function for the [internal_ParseFieldPart] function

   USAGE:
   SELECT * FROM [cron].[internal_ParseField]('0', 0, 10)            --Zero 
   SELECT * FROM [cron].[internal_ParseField]('0,5,7', 0, 10)        --Zero, 5 and 7
   SELECT * FROM [cron].[internal_ParseField]('1-4,3,8', 0, 10)      --1-4, 8 and 3 (duplicate) 
   SELECT * FROM [cron].[internal_ParseField]('*', 0, 10)            --All numbers between 0 and 10
   SELECT * FROM [cron].[internal_ParseField]('0-20/5', 0, 20)       --Every 5 numbers from 0 to 20
   SELECT * FROM [cron].[internal_ParseField]('0-20/5,18', 0, 20)    --Every 5 numbers from 0 to 20 and also 18
   SELECT * FROM [cron].[internal_ParseField]('5-1', 0, 100)         --wrong order, no result
   SELECT * FROM [cron].[internal_ParseField]('5,error/test', 0, 100)  --Results in error

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-10	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   FUNCTION [cron].[internal_ParseField]
(@cron varchar(255), @min int, @max int) 
RETURNS @result TABLE (numbers int)
AS
BEGIN
 --Unsorted list of parts in one segment
 DECLARE @parts TABLE (segment varchar(255))

 DECLARE @writablecron varchar(255) = @cron
 DECLARE @part varchar(255) 
 DECLARE @pos int

 WHILE CHARINDEX(',', @writablecron) > 0
 BEGIN

  SELECT @pos = CHARINDEX(',', @writablecron)
  SELECT @part = SUBSTRING(@writablecron, 1, @pos - 1)
  
  INSERT INTO @parts 
  SELECT @part

  SELECT @writablecron = SUBSTRING(@writablecron, @pos+1, LEN(@writablecron)-@pos)

 END
 INSERT INTO @parts 
 SELECT @writablecron

 --Return a distinct list of numbers that match the aggregated cron segment
 INSERT INTO @result(numbers) 
 SELECT DISTINCT s.number  
 FROM @parts p CROSS APPLY [cron].[internal_ParseFieldPart](p.segment, @min, @max) s
 
 RETURN
END
