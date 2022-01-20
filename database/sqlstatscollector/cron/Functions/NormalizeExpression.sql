

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[NormalizeExpression]
   -----------------------------------------
   Returns a cron expression without repeating spaces
   , where the month/day texts are replaced by numbers.

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-10	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   FUNCTION [cron].[NormalizeExpression]
(@cron varchar(255))
RETURNS varchar(255)
AS
BEGIN 
	DECLARE @cronexpression varchar(255)
	SET @cronexpression = @cron

	--Remove repeating whitespaces
	WHILE CHARINDEX('  ',@cronexpression) > 0
	BEGIN
		SET @cronexpression = REPLACE(@cronexpression, '  ',' ')
	END

	--Replace Month texts with numeric values
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'JAN', '1')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'FEB', '2')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'MAR', '3')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'APR', '4')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'MAY', '5')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'JUN', '6')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'JUL', '7')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'AUG', '8')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'SEP', '9')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'OCT', '10')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'NOV', '11')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'DEC', '12')

	--Replace day texts with numeric values
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'SUN', '0')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'MON', '1')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'TUE', '2')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'WED', '3')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'THU', '4')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'FRI', '5')
	SET @cronexpression = REPLACE(UPPER(@cronexpression), 'SAT', '6')

	RETURN @cronexpression
END
