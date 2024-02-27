SET NOCOUNT ON 
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/****** Section:  Schemas ******/
GO

/****** Object:  Schema [internal] ******/
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'internal')) 
BEGIN
    EXEC ('CREATE SCHEMA [internal] AUTHORIZATION [dbo]')
END
GO

/****** Object:  Schema [cron] ******/
IF (NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'cron')) 
BEGIN
    EXEC ('CREATE SCHEMA [cron] AUTHORIZATION [dbo]')
END
GO

/****** End Section:  Schemas ******/
GO

/****** Section:  Prerequisites ******/
GO

/****** Object:  Table-Value Function  [internal].[TableMetadataChecker] ******/
GO

IF OBJECT_ID(N'[internal].[TableMetadataChecker]', N'IF') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [internal].[TableMetadataChecker] () RETURNS TABLE AS RETURN SELECT x = NULL')
END
GO

/*******************************************************************************
--Copyright (c) 2024 Mikael Wedham (MIT License)
   -----------------------------------------
   [internal].[TableMetadataChecker]
   -----------------------------------------
   Calculates a checksum of the definition of a table.
   The checksum column is returned as a varbinary(32)

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-01-15	Mikael Wedham		+Created v1
2024-02-27	Mikael Wedham		+Added 2005-2014 compatibility
*******************************************************************************/
ALTER FUNCTION [internal].[TableMetadataChecker]
(@schemaname nvarchar(128), @tablename nvarchar(128), @tabledefinitionhash varbinary(32))
RETURNS TABLE
AS
RETURN
WITH ListOfTableMetadata AS
    (
        SELECT [schemaname] = @schemaname
		     , [tablename] = @tablename
             , [fullname] = '[' + schemainfo.[name] + '].[' + tableinfo.[name] + ']'
             , [columndata] = CAST((SELECT columnname = columninfo.[name]
										 , columninfo.[system_type_id]
										 , columninfo.[max_length]
										 , columninfo.[precision]
										 , columninfo.[scale]
										 , columninfo.[is_nullable]
										 , collation_name = ISNULL(columninfo.[collation_name],N'')
									FROM sys.columns columninfo 
									WHERE columninfo.[object_id] = tableinfo.[object_id]
									ORDER BY columnname
									FOR XML AUTO, ROOT(N'columns')) AS XML
								   )
			, [TableExists] = 1
		FROM sys.objects tableinfo 
        INNER JOIN sys.schemas schemainfo 
            ON tableinfo.[schema_id] = schemainfo.[schema_id]
        WHERE tableinfo.[type] = 'U' 
		  AND schemainfo.[name] = @schemaname
		  AND tableinfo.[name] = @tablename
		UNION ALL
        SELECT [schemaname] = @schemaname
		     , [tablename] = @tablename
			 , [fullname] = CAST('[' + @schemaname + '].[' + @tablename + ']' as nvarchar(256))
			 , [columndata] = CAST(NULL as XML)
			 , [TableExists] = 0
    ), CurrentTableDefinition AS
	(
		SELECT TOP(1) [SchemaName] = [schemaname]
			 , [TableName] = [tablename]
			 , [FullName] = [fullname]
			 , [TableDefinitionHash] = CAST(CASE WHEN [TableExists] = 0 THEN NULL ELSE HASHBYTES('SHA2_256', (SELECT fullname, columndata FROM (VALUES(NULL))keydata(x) FOR XML AUTO)) END AS varbinary(32))
			 , [TableExists]  = CAST([TableExists] AS int)
		FROM ListOfTableMetadata
		ORDER BY [TableExists] DESC
	)
		SELECT [SchemaName] = [SchemaName]
			 , [TableName] = [TableName]
			 , [FullName] = [FullName]
			 , [TableDefinitionHash] = [TableDefinitionHash]
			 , [TableExists] 
			 , [TableHasChanged] = CAST(CASE WHEN ISNULL(@tabledefinitionhash, 0x00) = [TableDefinitionHash] OR [TableExists] = 0 THEN 0 ELSE 1 END AS int)
		FROM CurrentTableDefinition
GO


/****** FUNCTION USAGE Example ******/

--DECLARE @SchemaName nvarchar(128) = N'internal'
--DECLARE @TableName nvarchar(128) = N'TableDefinitions'
--DECLARE @TableDefinitionHash varbinary(32) = 0xE38BB1615C5C08C0D8F8A584050077FE43B0A5932FE24262F7C8CAEEA514D064

--DECLARE @TableExists int
--DECLARE @TableHasChanged int
--SELECT @TableExists = [TableExists]
--     , @TableHasChanged = [TableHasChanged]
--FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

--IF @TableExists = 1 AND @TableHasChanged = 1
--BEGIN
--	RAISERROR(N'sp_rename of original table', 10, 1) WITH NOWAIT

--	DECLARE @NewName nvarchar(128)
--	SELECT @NewName = @TableName + N'_'
--	       + REPLACE(REPLACE(REPLACE(CONVERT(nvarchar(100), GETDATE(), 126), N'-', N''), N':', N''), N'.', N'')
--	ALTER TABLE [internal].[TableDefinitions] DROP CONSTRAINT [PK_internal_TableDefinitions]
--	EXECUTE sp_rename N'[internal].[TableDefinitions]', @NewName, 'OBJECT' 

--	SET @TableExists = 0
--END

--IF @TableExists = 0
--BEGIN
--	RAISERROR(N'Creating [internal].[TableDefinitions]', 10, 1) WITH NOWAIT
--	CREATE TABLE [internal].[TableDefinitions](
--		[SchemaName] [nvarchar](128) NOT NULL,
--		[TableName] [nvarchar](128) NOT NULL,
--		[ObjectDefinitionHash] varbinary(64) NOT NULL,
--	 CONSTRAINT [PK_internal_TableDefinitions] PRIMARY KEY CLUSTERED 
--		(
--			[SchemaName] ASC,
--			[TableName] ASC
--		) ON [PRIMARY]
--	) ON [PRIMARY]
--END
--GO

--IF @BackupTableCreated = 1
--BEGIN
--	SET NOCOUNT OFF
--	DECLARE @cmd nvarchar(400)
--	SELECT  @cmd = N'INSERT INTO [internal].[collectors]([section], [collector], [cron], [lastrun]) SELECT [section], [collector], [cron], [lastrun] FROM [internal].[' + @NewName +']'
--	EXEC (@cmd)
--	SELECT @cmd = N'DROP TABLE [internal].[' + @NewName +']'
--	EXEC (@cmd)
--	SET NOCOUNT ON
--END

/****** End Section:  Prerequisites ******/
GO


/****** Section:  Tables ******/
GO

/****** Object:  Table [cron].[Numbers] ******/
GO

DECLARE @SchemaName nvarchar(128) = N'cron'
DECLARE @TableName nvarchar(128) = N'Numbers'
DECLARE @TableDefinitionHash varbinary(32) = 0x4B7255B38744ED8B49E00ED404793D69DD2CF2CDB4D322317579BEC03EA0553C

DECLARE @TableExists int
DECLARE @TableHasChanged int
DECLARE @FullName nvarchar(255)

DECLARE @cmd nvarchar(2048)
DECLARE @msg nvarchar(2048)

SELECT @FullName = [FullName]
     , @TableExists = [TableExists]
     , @TableHasChanged = [TableHasChanged]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

IF @TableExists = 1 AND @TableHasChanged = 1
BEGIN
	RAISERROR(N'DROPPING original table', 10, 1) WITH NOWAIT
	DROP TABLE [cron].[Numbers] 
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	RAISERROR(N'Creating [cron].[Numbers]', 10, 1) WITH NOWAIT
	CREATE TABLE [cron].[Numbers](
		[number] [int] NOT NULL,
	 CONSTRAINT [PK_cron_numbers] PRIMARY KEY CLUSTERED 
		(
			[number] ASC
		) ON [PRIMARY]
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

GO


--/****** Object:  Table [cron].[Dates] ******/
GO

DECLARE @SchemaName nvarchar(128) = N'cron'
DECLARE @TableName nvarchar(128) = N'Dates'
DECLARE @TableDefinitionHash varbinary(32) = 0x0B8C6F1DCC18D642B5E8594D5AD2DEC7D301CF0E48399C77F78F83229713251B

DECLARE @TableExists int
DECLARE @TableHasChanged int
DECLARE @FullName nvarchar(255)
DECLARE @NewName nvarchar(128)

DECLARE @cmd nvarchar(2048)
DECLARE @msg nvarchar(2048)

SELECT @FullName = [FullName]
     , @TableExists = [TableExists]
     , @TableHasChanged = [TableHasChanged]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)

IF @TableExists = 1 AND @TableHasChanged = 1
BEGIN
	RAISERROR(N'DROPPING original table', 10, 1) WITH NOWAIT
	DROP TABLE [cron].[Dates] 
	SET @TableExists = 0
END

IF @TableExists = 0
BEGIN
	RAISERROR(N'Creating [cron].[Dates]', 10, 1) WITH NOWAIT
	CREATE TABLE [cron].[Dates](
		[datevalue] [date] NOT NULL,
		[yearvalue] [int] NOT NULL,
		[monthvalue] [int] NOT NULL,
		[dayvalue] [int] NOT NULL,
		[weekdayvalue] [int] NOT NULL,
	 CONSTRAINT [PK_cron_Dates] PRIMARY KEY CLUSTERED 
		(
			[datevalue] ASC
		) ON [PRIMARY]
	) ON [PRIMARY]
END

SELECT FullName = [FullName]
     , TableDefinitionHash = [TableDefinitionHash]
FROM [internal].[TableMetadataChecker](@SchemaName, @TableName, @TableDefinitionHash)
GO

/****** End Section:  Tables ******/
GO

/****** Section:  Stored Procedures ******/
GO

/****** Object:  StoredProcedure [cron].[FillNumbers] ******/
GO

IF OBJECT_ID(N'cron.FillNumbers', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [cron].[FillNumbers] AS SELECT NULL')
END
GO

/*******************************************************************************
--Copyright (c) 2024 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[FillNumbers]
   -----------------------------------------
   Fills the Numbers-table with many numbers (default 10000)

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-01-16	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [cron].[FillNumbers]
(@NumberCount int = 10000)
AS
BEGIN
--Create a sequence of numbers between zero and @numbercount
;WITH num AS
		(
		 SELECT number = 0 
		   UNION ALL 
		 SELECT number = number + 1 
		 FROM num 
		 WHERE number < @NumberCount
		 )

	--Merge the number sequence, adding missing numbers to the table
	MERGE [cron].[Numbers] n USING num src
		ON n.number = src.number
	WHEN NOT MATCHED THEN
		INSERT ([number])
		VALUES (src.number)
	OPTION (maxrecursion 0);
END
GO

/****** Object:  StoredProcedure [cron].[FillDates] ******/
GO

IF OBJECT_ID(N'cron.FillDates', N'P') IS NULL
BEGIN
	EXEC ('CREATE PROCEDURE [cron].[FillDates] AS SELECT NULL')
END
GO

/*******************************************************************************
--Copyright (c) 2024 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[FillDates]
   -----------------------------------------
   Fills the Dates-table with dates, to be used for schedule calculations

   Parameters:
    @FutureNumberOfDates = This is how far in the future dates should be added
	@PreviousNumberOfDates = This value represents the number of past dates to be inserted 

Date		Name				Description
----------	-------------		-----------------------------------------------
2024-01-16	Mikael Wedham		+Created v1
*******************************************************************************/
ALTER PROCEDURE [cron].[FillDates]
(
  @FutureNumberOfDates int = 10000 --Number of days
, @PreviousNumberOfDates int = -366
)
AS
BEGIN
		--Get a value in order to work with all DATEFIRST settings
		DECLARE @deltaday int
		SELECT @deltaday = @@DATEFIRST - 1

	--Create a sequence of numbers between @PreviousNumberOfDates and @FutureNumberOfDates
	;WITH datenum AS --Get a number list for the estimated number of days
		(SELECT number = @PreviousNumberOfDates 
		   UNION ALL 
		 SELECT number = number + 1 
		 FROM datenum 
		 WHERE number < @FutureNumberOfDates)
	,datesequence AS --Create the date list, with the last run date as the base.
		(SELECT number
			  , dt = CAST(DATEADD(DAY, number, SYSUTCDATETIME()) AS date)
		 FROM datenum )
	, datelist AS
	(SELECT dt
	       ,y = YEAR(dt)
	       ,m = MONTH(dt)
           ,d = DAY(dt)
		   ,dw = (DATEPART(WEEKDAY, dt) + @deltaday) % 7		--Modulo accounts for overflowing daynumbers if DATEFIRST > 1
	FROM datesequence)

	MERGE [cron].[Dates] d USING datelist src
	ON d.[datevalue] = src.dt
	WHEN NOT MATCHED THEN
		INSERT ([datevalue],[yearvalue],[monthvalue],[dayvalue],[weekdayvalue])
		VALUES (src.dt, src.y, src.m, src.d, src.dw)
	WHEN MATCHED AND d.[weekdayvalue] != src.dw THEN
		UPDATE SET [weekdayvalue] = src.dw
	OPTION (maxrecursion 0);

END
GO

/****** End Section:  Stored Procedures ******/
GO

/****** Section:  User Defined Functions ******/
GO

/****** Object:  UserDefinedFunction [cron].[NormalizeExpression] ******/
GO

IF OBJECT_ID(N'[cron].[NormalizeExpression]', N'FN') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [cron].[NormalizeExpression] () RETURNS varchar(255) AS BEGIN RETURN NULL END')
END
GO

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
ALTER FUNCTION [cron].[NormalizeExpression]
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
GO


/****** Object:  UserDefinedFunction [cron].[internal_ParseRangeExpression] ******/
GO

IF OBJECT_ID(N'[cron].[internal_ParseRangeExpression]', N'TF') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [cron].[internal_ParseRangeExpression] () RETURNS @result TABLE (x int) AS BEGIN RETURN END')
END
GO

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
ALTER FUNCTION [cron].[internal_ParseRangeExpression]
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
GO



/****** Object:  UserDefinedFunction [cron].[internal_ParseEveryNExpression] ******/
GO

IF OBJECT_ID(N'[cron].[internal_ParseEveryNExpression]', N'TF') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [cron].[internal_ParseEveryNExpression] () RETURNS @result TABLE (x int) AS BEGIN RETURN END')
END
GO


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
ALTER FUNCTION [cron].[internal_ParseEveryNExpression]
(@cron varchar(255)) 
RETURNS @result TABLE (cron varchar(255), everyn int)
AS
BEGIN
	DECLARE @cronpattern varchar(255)
	DECLARE @modulo int

	DECLARE @splitter int
	--Find the position of the '/' separator
	SELECT @splitter = CHARINDEX('/', @cron)

	--If separator is duplicated or missing, this is not a correct cron part : exit.
	IF ( SELECT COUNT(*) FROM STRING_SPLIT(@cron, '/')) <> 2 OR (@splitter = 0)
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
GO




/****** Object:  UserDefinedFunction [cron].[internal_ParseFieldPart] ******/
GO

IF OBJECT_ID(N'[cron].[internal_ParseFieldPart]', N'TF') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [cron].[internal_ParseFieldPart] () RETURNS @result TABLE (x int) AS BEGIN RETURN END')
END
GO

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
2023-11-27	Mikael Wedham		+Refactored to use internal Numbers table
*******************************************************************************/
ALTER FUNCTION [cron].[internal_ParseFieldPart]
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
 	SELECT n.[number]
	FROM [cron].[Numbers] n
	WHERE n.[number] BETWEEN @start AND @stop
	  AND (n.[number] % ISNULL(@EveryN, 1) = 0 OR ISNULL(@EveryN, 1) = 1)
 
 RETURN
END
GO




/****** Object:  UserDefinedFunction [cron].[internal_ParseField] ******/
GO

IF OBJECT_ID(N'[cron].[internal_ParseField]', N'IF') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [cron].[internal_ParseField] () RETURNS TABLE AS RETURN SELECT x = NULL')
END
GO

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
2023-11-27	Mikael Wedham		+Rewrite as Inline TVF
*******************************************************************************/
ALTER FUNCTION [cron].[internal_ParseField]
(@cron varchar(255), @min int, @max int) 
RETURNS TABLE 
AS
RETURN
(
 --Return a distinct list of numbers that match the aggregated cron segment
 SELECT DISTINCT s.number  
 FROM STRING_SPLIT(@cron, ',') p CROSS APPLY [cron].[internal_ParseFieldPart](p.value, @min, @max) s
)

GO



/****** Object:  UserDefinedFunction [cron].[GetNext]    Script Date: 2023-11-28 11:21:27 ******/
GO

IF OBJECT_ID(N'[cron].[GetNext]', N'FN') IS NULL
BEGIN
	EXEC ('CREATE FUNCTION [cron].[GetNext] () RETURNS datetime AS BEGIN RETURN NULL END')
END
GO

/*******************************************************************************
--Copyright (c) 2022 Mikael Wedham (MIT License)
   -----------------------------------------
   [cron].[GetNext]
   -----------------------------------------
   Gets the next run date and time for this cron expression
   with the starting point entered as parameter

   USAGE:
   SELECT [cron].[GetNext]('* * * * *', '2022-02-20 18:00:00')   --Each minute, Get next schedule after 6PM on Feb 20th 2022.
   SELECT [cron].[GetNext]('59 23 31 12 5',  NULL)               --Get next schedule from NOW One minute before the end of year if the last day of the year is Friday
   SELECT [cron].[GetNext]('45 17 7 6 * ', NULL)                 --Every year, on June 7th at 17:45 , start date NOW
   SELECT [cron].[GetNext]('0 12 * * 1-5', '2024-01-14')         --At midday on weekdays, returns Monday (on Sat & Sun)
   SELECT [cron].[GetNext]('* /15 * /6 1,15,31 * 1-5', NULL)   --At 00:00, 00:15, 00:30, 00:45, 06:00, 06:15, 06:30, 06:45, 12:00, 12:15, 12:30, 12:45, 18:00, 18:15, 18:30, 18:45
                                                                   , on 1st, 15th or  31st of each  month
																   , but not on weekends , starting now
																   --SPACES ADDED TO FIRST AND SECOND PARAMETER IN EXPRESSION DUE TO COMMENT ISSUES IN SQL
																   -- REMOVE SPACE TO THE LEFT OF THE SLASH (/) TO MAKE THIS SAMPLE RUNNABLE

Date		Name				Description
----------	-------------		-----------------------------------------------
2022-01-04	Mikael Wedham		+Created v1
2022-03-29	Mikael Wedham		+Added StartDate parameter to get a better view 
                                on how delayed a schedule is.
								@StartDate = NULL means that the function uses
								GETDATE() to get the StartDate
2023-11-27	Mikael Wedham		+Refactored for performance
2024-01-17	Mikael Wedham		+Changed function to be a Scalar UDF
*******************************************************************************/
ALTER FUNCTION [cron].[GetNext]
(@cron varchar(255), @StartDate datetime = NULL) 
RETURNS datetime
AS
BEGIN
	DECLARE @result datetime

	IF (@StartDate IS NULL)
	BEGIN
		SET @StartDate = GETDATE()
	END
	
	DECLARE @LastExec date
	SET @LastExec = @StartDate

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
		INSERT @tMinutes (value) SELECT number FROM [cron].[internal_ParseField](@minute, 0, 59);

		--Get a list of all hours in the expression
		DECLARE @tHours TABLE (value int)
		INSERT @tHours (value) SELECT number*60 FROM [cron].[internal_ParseField](@hour, 0, 23);

		--Get a list of all days of the month in the expression
		DECLARE @tDays TABLE (value int)
		INSERT @tDays (value) SELECT number FROM [cron].[internal_ParseField](@dayofmonth, 1, 31);

		--Get a list of all months in the expression
		DECLARE @tMonths TABLE (value int)
		INSERT @tMonths (value) SELECT number FROM [cron].[internal_ParseField](@month, 1, 12);

		--Get a list of all allowed days of the week in the expression
		DECLARE @tWeekdays TABLE (value int)
		INSERT @tWeekdays (value) SELECT number FROM [cron].[internal_ParseField](@dayofweek, 0, 7);

		--Get a value in order to work with all DATEFIRST settings
		DECLARE @deltaday int
		SELECT @deltaday = @@DATEFIRST - 1

    --Contains all dates that should have at least one scheduled time
	DECLARE @tdates TABLE (value date)

	INSERT INTO @tdates(value)
	SELECT TOP(2) dates.datevalue 
	FROM cron.Dates dates INNER JOIN @tMonths m ON m.value = dates.monthvalue --Only get dates for the selected months
		INNER JOIN @tDays d ON d.value = dates.dayvalue --Only get dates for the selected day of month
		INNER JOIN @tWeekdays w ON w.value = dates.weekdayvalue --Only get dates for the selected day of week.
	WHERE dates.datevalue >= @LastExec
	ORDER BY dates.datevalue

	;WITH allScheduledTimes AS --Generate datetime values for all dates and times
	  ( SELECT schedule = DATEADD(MINUTE, (hh.value + mm.value), CAST(d.value as datetime))
	    FROM @tdates d CROSS JOIN @tHours hh CROSS JOIN @tMinutes mm)
    , NextSchedule AS --Get the next expected schedule
	(SELECT DISTINCT schedule
	FROM allScheduledTimes
	WHERE schedule >= @StartDate
	)
	SELECT @result = MIN(schedule)
	FROM NextSchedule

RETURN  @result

END
GO



/****** End Section:  User Defined Functions ******/
GO

/****** Section:  Data Initialization ******/
GO

SET NOCOUNT OFF
GO
RAISERROR(N'Adding initial data to Numbers and Dates', 10, 1) WITH NOWAIT
GO
EXEC [cron].[FillNumbers]
GO
EXEC [cron].[FillDates]
GO
SET NOCOUNT ON
GO
/****** End Section:  Data Initialization ******/
GO



RAISERROR(N'CRON features added', 10, 1) WITH NOWAIT
GO
