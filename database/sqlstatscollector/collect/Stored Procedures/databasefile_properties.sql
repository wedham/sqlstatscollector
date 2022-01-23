/*******************************************************************************
Copyright (c) 2022 Mikael Wedham (MIT License)
   ---------------------------------------
   [collect].[databasefile_properties]
   ---------------------------------------
   Collects all properties of individual database files.
   One row per file, updates only - no history.
   All filesizes are in whole Megabytes.
   Removed files are kept until cleanup is initiated

Date		Name				Description
--------	-------------		-----------------------------------------------
2022-01-23	Mikael Wedham		+Created v1
*******************************************************************************/
CREATE PROCEDURE [collect].[databasefile_properties]
AS
BEGIN
PRINT('[collect].[databasefile_properties] - Get all properties of individual database files')
SET NOCOUNT ON

/* Only one statement is needed */
MERGE [data].[databasefile_properties] dest USING 
(SELECT f.[database_id]
      , f.[file_id]
	  , f.[type_desc] 
	  , f.[name]
      , f.[physical_name] 
	  , f.[state_desc] 
	  , [size_mb] = CAST( (f.[size]/128.0) AS decimal(19,4) ) 
	  , [max_size_mb] = CASE WHEN f.[max_size] = -1 THEN NULL
	                         ELSE CAST( f.[max_size]/128.0 AS int) END --Null means no limit
	  , [growth_mb] = CASE WHEN f.[is_percent_growth] = 1 THEN NULL
	                       ELSE CAST(f.[growth]/128.0 as int) END --NULL if growth is percent
	  , [growth_percent] = CASE WHEN f.[is_percent_growth] = 0 THEN NULL
	                       ELSE f.[growth] END --NULL if growth is in megabytes
 FROM sys.master_files f ) src
 ON src.[database_id] = dest.[database_id] AND src.[file_id] = dest.[file_id]  
WHEN NOT MATCHED THEN
    INSERT ([database_id], [file_id], [type_desc], [name], [physical_name], [state_desc]
	      , [size_mb], [max_size_mb] ,[growth_mb] ,[growth_percent] ,[LastUpdated]) 
    VALUES (src.[database_id], src.[file_id], src.[type_desc], src.[name], src.[physical_name], src.[state_desc]
	      , src.[size_mb], src.[max_size_mb] ,src.[growth_mb] ,src.[growth_percent] ,SYSUTCDATETIME()) 
WHEN MATCHED THEN 
    UPDATE SET
       [type_desc] = src.[type_desc]
      ,[name] = src.[name]
      ,[physical_name] = src.[physical_name]
      ,[state_desc] = src.[state_desc]
      ,[size_mb] = src.[size_mb]
      ,[max_size_mb] = src.[max_size_mb]
      ,[growth_mb] = src.[growth_mb]
      ,[growth_percent] = src.[growth_percent]
      ,[LastUpdated] = SYSUTCDATETIME()
WHEN NOT MATCHED BY SOURCE THEN --File was removed from the database
    UPDATE SET
       [type_desc] = N'NONE'
      ,[name] = N'*removed*'
      ,[physical_name] = N'*removed*'
      ,[state_desc] = N'REMOVED'
      ,[size_mb] = 0
      ,[max_size_mb] = NULL
      ,[growth_mb] = NULL
      ,[growth_percent] = NULL
      ,[LastUpdated] = SYSUTCDATETIME();

END