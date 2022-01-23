

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[internal_ParseEveryNExpression]
   -----------------------------------------
   Returns the Every N parameter based on a cron partition expression
   The results includes the range/number and the Every N number in the expression
   An Every N expression is a cron expression followed by a division sign : '0-5/2'

   USAGE:
   SELECT * FROM [cron].[internal_ParseEveryNExpression]('0/1')
   SELECT * FROM [cron].[internal_ParseEveryNExpression]('0-23/5')
   SELECT * FROM [cron].[internal_ParseEveryNExpression]('5-1')         --separator missing
   SELECT * FROM [cron].[internal_ParseEveryNExpression]('error/test')  --Results in error

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-10	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   FUNCTION [cron].[internal_ParseEveryNExpression]
(@cron varchar(255)) 
RETURNS @result TABLE (cron varchar(255), everyn int)
AS
BEGIN
	DECLARE @cronpattern varchar(255)
	DECLARE @modulo int

	DECLARE @splitter int
	DECLARE @splitter2 int
	--Find the position of the '/' separator
	SELECT @splitter = CHARINDEX('/', @cron)
	SELECT @splitter2 = CHARINDEX('/', @cron, @splitter + 1)

	--If separator is duplicated or missing, this is not a correct cron part : exit.
	IF (@splitter = 0) OR (@splitter2 > 0)
	BEGIN
	   RETURN
	END

	--Get the first part, that is the base cron pattern
	SET @cronpattern = SUBSTRING(@cron, 1, @splitter-1)
	--Get the partition/EveryN value
    SET @modulo = CAST(SUBSTRING(@cron, @splitter+1, LEN(@cron)) AS int)

	--Return the new pattern and the EveryN parameter separately
	INSERT INTO @result(cron, everyn) SELECT @cronpattern, @modulo

	RETURN
END
