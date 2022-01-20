
/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[internal_GetNumbers]
   -----------------------------------------
   Base functionality of the interval parser
   Returns a list of numbers between the parameters Min and Max. 
   EveryN parameter selects Every N rows only.

   USAGE:
   --Get every number between 1 and 5
   SELECT * FROM [cron].[internal_GetNumbers](1, 5, 1)
   --Get every third number between 1 and 24
   SELECT * FROM [cron].[internal_GetNumbers](0, 23, 3)
   --Get every 10th number between 3 and 35
   SELECT * FROM [cron].[internal_GetNumbers](3, 35, 10)

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-10	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE   FUNCTION [cron].[internal_GetNumbers]
(@Min int, @Max int, @EveryN int) 
RETURNS @result TABLE (number int)
AS
BEGIN
	--Assume NULL means every value
    SET @EveryN = ISNULL(@EveryN, 1)
	--Ranges must be entered left-to-right
    IF @Max >= @Min
	BEGIN
	     --Recursive CTE with counter column for use with EveryN functionality
  		WITH Starter(mv, ctr) AS (
			SELECT @Min, 0 --Root value
			UNION ALL
			SELECT mv + 1, ctr + 1 --Increment values
			FROM Starter --Recursive connection
			WHERE mv + 1 <= @Max --End value
		)
		INSERT @result (number) --Prepare results table
		SELECT mv 
		FROM Starter
		WHERE ctr % @EveryN = 0; --Modulo calculation on selector returns only the values wanted
	END
  RETURN
END
