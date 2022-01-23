﻿CREATE   FUNCTION [cron].[GetPreviousSchedule]
(@cron varchar(255)) 
RETURNS @result TABLE (scheduledtime datetime)
AS
BEGIN
	--Create writable cron
	DECLARE @cronexpression varchar(255)
	SET @cronexpression = [cron].[NormalizeExpression](@cron)

	DECLARE @minute varchar(255)
	DECLARE @minutepos int
	DECLARE @hour varchar(255)
	DECLARE @hourpos int
	DECLARE @dayofmonth varchar(255)
	DECLARE @dayofmonthpos int
	DECLARE @month varchar(255)
	DECLARE @monthpos int
	DECLARE @dayofweek varchar(255)

	--Get positions of all parts 
	SELECT @minutepos = CHARINDEX(' ', @cronexpression, 1) - 1
	SELECT @hourpos = CHARINDEX(' ', @cronexpression, @minutepos+2) - 1
	SELECT @dayofmonthpos = CHARINDEX(' ', @cronexpression, @hourpos+2) - 1
	SELECT @monthpos = CHARINDEX(' ', @cronexpression, @dayofmonthpos+2) - 1

	--Extract the different parts of the cron expression
	SELECT @minute = SUBSTRING(@cronexpression, 1, @minutepos) 
	, @hour = SUBSTRING(@cronexpression, @minutepos + 2, @hourpos-@minutepos) 
	, @dayofmonth = SUBSTRING(@cronexpression,@hourpos + 2 , @dayofmonthpos-@hourpos) 
	, @month = SUBSTRING(@cronexpression, @dayofmonthpos + 2, @monthpos-@dayofmonthpos) 
	, @dayofweek = SUBSTRING(@cronexpression,@monthpos + 2, LEN(@cronexpression)-@monthpos+1) 

		--Get a list of all minutes in the expression
		DECLARE @tMinutes TABLE (value int)
		INSERT @tMinutes (value) SELECT numbers FROM [cron].[internal_ParseField](@minute, 0, 59);

		--Get a list of all hours in the expression
		DECLARE @tHours TABLE (value int)
		INSERT @tHours (value) SELECT numbers FROM [cron].[internal_ParseField](@hour, 0, 23);

		--Get a list of all days of the month in the expression
		DECLARE @tDays TABLE (value int)
		INSERT @tDays (value) SELECT numbers FROM [cron].[internal_ParseField](@dayofmonth, 1, 31);

		--Get a list of all months in the expression
		DECLARE @tMonths TABLE (value int)
		INSERT @tMonths (value) SELECT numbers FROM [cron].[internal_ParseField](@month, 1, 12);

		--Get a list of all allowed days of the week in the expression
		DECLARE @tWeekdays TABLE (value int)
		INSERT @tWeekdays (value) SELECT numbers FROM [cron].[internal_ParseField](@dayofweek, 0, 7);

		--Get a value in order to work with all DATEFIRST settings
		DECLARE @deltaday int
		SELECT @deltaday = @@DATEFIRST - 1

	--Do not calculate more days than needed (rough estimate)
	DECLARE @days int = 7
	
    --Contains all dates that should have at least one scheduled time
	DECLARE @tdates TABLE (value date)
	
	WHILE (SELECT COUNT(*) FROM @tdates) < 1 OR @days > 2500000
	BEGIN
		;WITH datenum AS --Get a number list for the estimated number of days
			(SELECT number = 1 
			   UNION ALL 
			 SELECT number = number - 1 
			 FROM datenum 
			 WHERE number > -@days)
		,datesequence AS --Create the date list, with the last run date as the base.
			(SELECT number
				  , dt = DATEADD(DAY, number, GETDATE())
			 FROM datenum )

		--Get all the dates from the date list that match the date filter
		--Modulo accounts for overflowing daynumbers if DATEFIRST > 1
		INSERT INTO @tdates(value)
		SELECT dates.dt 
		FROM datesequence dates INNER JOIN @tMonths m ON m.value = DATEPART(MONTH, dates.dt) --Only get dates for the selected months
			INNER JOIN @tDays d ON d.value = DATEPART(DAY, dates.dt) --Only get dates for the selected day of month
			INNER JOIN @tWeekdays w ON w.value = (DATEPART(WEEKDAY, dates.dt) + @deltaday) % 7 --Only get dates for the selected day of week. 
		option (maxrecursion 0)

		SET @days = @days * 2
    END

	--Contains all scheduled times
	DECLARE @ttimes TABLE (value time(0))

	--Generate all time values, by combining all hours and all minutes
	INSERT INTO @ttimes(value)
	SELECT t = RIGHT('0' + CAST(h.value as varchar(2)), 2) + ':' + RIGHT('0' + CAST(m.value as varchar(2)), 2) + ':00'
	FROM @tHours h CROSS JOIN @tMinutes m

	;WITH allScheduledTimes AS --Generate datetime values for all dates and times
	  ( SELECT schedule = CAST(CONVERT(varchar(20), d.value, 121) + ' ' + CONVERT(varchar(20), t.value) AS datetime)
	    FROM @tdates d CROSS JOIN @ttimes t)
    , PreviousSchedule AS --Get the next expected schedule
	(SELECT TOP(1)  schedule
	FROM allScheduledTimes
	WHERE schedule < GETDATE()
	ORDER BY schedule DESC)

	INSERT INTO @result(scheduledtime)
	SELECT schedule
	FROM PreviousSchedule

	RETURN  

END
