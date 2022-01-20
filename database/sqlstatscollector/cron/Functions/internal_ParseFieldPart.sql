


/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[internal_ParseFieldPart]
   -----------------------------------------
   Returns a list of numbers based on a cron expression field part
   A field part is ONE of the comma separated items in a cron field.
   This function is a wrapper function for the *ParseEveryN* and *ParseRange* functions

   USAGE:
   SELECT * FROM [cron].[internal_ParseFieldPart]('0', 0, 10)            --Zero 
   SELECT * FROM [cron].[internal_ParseFieldPart]('*', 0, 10)            --All numbers between 0 and 10
   SELECT * FROM [cron].[internal_ParseFieldPart]('0-20/5', 0, 20)       --Every 5 numbers from 0 to 20
   SELECT * FROM [cron].[internal_ParseFieldPart]('* / 5', 0, 20)        --Every 5 numbers from 0 to 20 (extra spaces added due to T-SQL comments)
   SELECT * FROM [cron].[internal_ParseFieldPart]('5-1', 0, 100)         --wrong order, no result
   SELECT * FROM [cron].[internal_ParseFieldPart]('error/test', 0, 100)  --Results in error


Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-10	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   FUNCTION [cron].[internal_ParseFieldPart]
(@cron varchar(255), @min int, @max int) 
RETURNS @result TABLE (number int)
AS
BEGIN

 DECLARE @start int 
 DECLARE @stop int 
 DECLARE @EveryN int = 1
 DECLARE @cronpart varchar(255)

 --Get a writable copy of the cron expression
 SET @cronpart = @cron
 
 --If expression contains /, it is a partition/EveryN expression
 IF (CHARINDEX('/', @cron) > 0)
 BEGIN
    --Set the new cron expression and keep the EveryN value
	SELECT @cronpart = cron, @EveryN = everyn 
	FROM [cron].[internal_ParseEveryNExpression](@cron)
 END 
 
 --If expression contains -, it is a range expression
 IF (CHARINDEX('-', @cronpart) > 0)
 BEGIN
    --Get the start/stop values from the range.
	SELECT @start = fromnumber, @stop = tonumber 
	FROM [cron].[internal_ParseRangeExpression](@cronpart)
 END
 
 --A star indicates full range (between min and max)
 IF (@cronpart = '*')
 BEGIN
	SELECT @start = @min, @stop = @max
 END

 --If the part is a single number, min and max are equal
 IF (ISNUMERIC(@cronpart) = 1)
 BEGIN
    SELECT @start = CAST(@cronpart AS int), @stop = CAST(@cronpart AS int)
 END

 --Prepare the results based on the full parsing of the cron expression
 INSERT INTO @result(number) 
 SELECT number 
 FROM [cron].[internal_GetNumbers](@start, @stop, @EveryN)
 
 RETURN
END
