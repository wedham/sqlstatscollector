

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[internal_ParseRangeExpression]
   -----------------------------------------
   Returns the range of numbers based on a cron range expression
   The results includes the from and to numbers in the expression
   A Range expression is 2 numbers separated by a minus sign : '2-5'

--Copyright (c) 2022 Mikael Wedham

--Permission is hereby granted, free of charge, to any person obtaining a copy
--of this software and associated documentation files (the "Software"), to deal
--in the Software without restriction, including without limitation the rights
--to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
--copies of the Software, and to permit persons to whom the Software is
--furnished to do so, subject to the following conditions:
--
--The above copyright notice and this permission notice shall be included in all
--copies or substantial portions of the Software.
--
--THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
--IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
--FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
--AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
--LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
--OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
--SOFTWARE.

   USAGE:
   SELECT * FROM [cron].[internal_ParseRangeExpression]('2-5')
   SELECT * FROM [cron].[internal_ParseRangeExpression]('0-23')
   SELECT * FROM [cron].[internal_ParseRangeExpression]('5-1')       --Ordering is wrong
   SELECT * FROM [cron].[internal_ParseRangeExpression]('25')        --No actual range
   SELECT * FROM [cron].[internal_ParseRangeExpression]('an-error')  --Results in error

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-10	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   FUNCTION [cron].[internal_ParseRangeExpression]
(@cron varchar(255)) 
RETURNS @result TABLE (fromnumber int, tonumber int)
AS
BEGIN
	DECLARE @from int 
	DECLARE @to int 

	DECLARE @splitter int
	--Find the position of the '-' separator
	SELECT @splitter = CHARINDEX('-', @cron)

	--If separator is duplicated or missing, this is not a correct cron part : exit.
	IF ( SELECT COUNT(*) FROM STRING_SPLIT(@cron, '-')) <> 2 OR (@splitter = 0)
	BEGIN
	   RETURN
	END

	--Get the 2 parts of the range expression
	SET @from = CAST(SUBSTRING(@cron, 1, @splitter-1) AS int)
    SET @to = CAST(SUBSTRING(@cron, @splitter+1, LEN(@cron)) AS int)

	IF (@to < @from) --Return nothing when numbers are in the wrong order
	BEGIN
	  RETURN
	END

	--Return parts as a table.
	INSERT INTO @result(fromnumber, tonumber) SELECT @from, @to 

	RETURN
END
