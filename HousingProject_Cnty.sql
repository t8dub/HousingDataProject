

--ETL (Extract/Transform/Load) project using SSMS (not SSIS this time)>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


/*
1) Built 'New Database', named it HousingDataProject ------------------------------------------------------

2) Imported source data w/ SQL Server Imp/Exp Wizard ------------------------------------------------------

Using 'Redfin Housing Market Data 2012-2021' via Kaggle
(data courtesy of Redfin, a national real estate brokerage)
https://www.kaggle.com/datasets/thuynyle/redfin-housing-market-data?resource=download
Format was a zipped tab-separated flat file (.tsv)
	Copied to [dbo].[county_market_tracker]
	5310672 rows transferred 

After discovering omissions in the Kaggle dataset above, I subsequently imported a newer, 
more complete file directly from Redfin.
https://www.redfin.com/news/data-center/
https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz
Format was a zipped tab-seperated flat file (.tsv000.gz)
	Copied to [dbo].[new_zip_code_market_tracker]
	7143006 rows transferred 
*/

--3) Exploring data ----------------------------------------------------------------------------------------

--First look
SELECT TOP (1000) *
  FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([period_duration]), Count([period_duration])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [period_duration] order by 2 desc
-- all=30, don't need it

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct(region_type), Count(region_type)
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by region_type order by 2 desc
-- all='county', don't need it

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([region_type_id]), Count([region_type_id])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [region_type_id] order by 2 desc
-- all=5, don't need it

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([table_id]), Count([table_id])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [table_id] order by 2 desc
-- many different types...

-- ...check against [region] (aka county) 
SELECT TOP (1000) [table_id], [region]
  FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
  order by 2 desc
-- table_id appears to delineate county maybe keep as county id

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([is_seasonally_adjusted]), Count([is_seasonally_adjusted])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [is_seasonally_adjusted] order by 2 desc
-- all='f', don't need it

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([city]), Count([city])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [city] order by 2 desc
-- all=' ', don't need it

-- Confirming all of a given [state] matches a single [state_code] and vice versa
Select Distinct [state], [state_code]
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
order by 2 desc
-- only need state_code, but data is missing ND, WY 

-- Checking attributes for unique data (to determine whether to keep in next step) 
SELECT TOP (1000) [property_type], [property_type_id]
  FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
  order by 2
-- [property_type], [property_type_id] appear to show the same thing (don't need both)...

-- Confirming all of a given [property_type_id] match a single [property_type] and vice versa
Select Distinct property_type, property_type_id, COUNT(*)
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
GROUP BY property_type, property_type_id
order by 2 desc
-- keep [property_type_id] 

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([months_of_supply]), Count([months_of_supply])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [months_of_supply] order by 2 desc
-- might come in handy - keep it

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([price_drops]), Count([price_drops])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [price_drops] order by 2 desc
-- might come in handy - keep it

-- Checking attributes for unique data (to determine whether to keep in next step) 
Select Distinct([last_updated]), Count([last_updated])
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
Group by [last_updated] order by 2 desc
-- all='2023-08-21', don't need it.


-- Working list of attributes to keep
 /* 
	  ,[period_end]
      ,[table_id]
      ,[region]
      ,[state_code]
      ,[property_type]
      ,[property_type_id]
      ,[median_sale_price]
      ,[median_sale_price_mom]
      ,[median_sale_price_yoy]
      ,[median_list_price]
      ,[median_list_price_mom]
      ,[median_list_price_yoy]
      ,[median_ppsf]
      ,[median_list_ppsf]
      ,[homes_sold]
      ,[pending_sales]
      ,[new_listings]
      ,[inventory]
      ,[months_of_supply]
      ,[median_dom]
      ,[avg_sale_to_list]
      ,[sold_above_list]
      ,[price_drops]
      ,[off_market_in_two_weeks]
      ,[parent_metro_region]
      ,[parent_metro_region_metro_code]
 
*/
--4) Dropping unneccessary attributes (columns) -------------------------------------------------------------

 ALTER TABLE [dbo].[county_market_tracker]
 DROP COLUMN [period_begin]
      ,[period_duration]
      ,[region_type]
      ,[region_type_id]
      ,[is_seasonally_adjusted]
      ,[city]
      ,[state]
      ,[median_ppsf_mom]
      ,[median_ppsf_yoy]
      ,[median_list_ppsf_mom]
      ,[median_list_ppsf_yoy]
      ,[homes_sold_mom]
      ,[homes_sold_yoy]
      ,[pending_sales_mom]
      ,[pending_sales_yoy]
      ,[new_listings_mom]
      ,[new_listings_yoy]
      ,[inventory_mom]
      ,[inventory_yoy]
      ,[months_of_supply_mom]
      ,[months_of_supply_yoy]
      ,[median_dom_mom]
      ,[median_dom_yoy]
      ,[avg_sale_to_list_mom]
      ,[avg_sale_to_list_yoy]
      ,[sold_above_list_mom]
      ,[sold_above_list_yoy]
      ,[price_drops_mom]
      ,[price_drops_yoy]
      ,[off_market_in_two_weeks_mom]
      ,[off_market_in_two_weeks_yoy]
      ,[last_updated];
-- Commands completed successfully.

-- Checking for NULLs
 SELECT  COUNT(*)
  FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
  WHERE [period_end] IS NULL OR
      [region] IS NULL OR
      [state_code] IS NULL OR
      [property_type] IS NULL OR
      [median_sale_price] IS NULL OR
      [median_list_price] IS NULL OR
      [median_ppsf] IS NULL OR
      [median_list_ppsf] IS NULL OR
      [homes_sold] IS NULL OR
      [pending_sales] IS NULL OR
      [new_listings] IS NULL OR
      [inventory] IS NULL OR
      [median_dom] IS NULL OR
      [avg_sale_to_list] IS NULL OR
      [sold_above_list] IS NULL OR
      [off_market_in_two_weeks] IS NULL OR
      [parent_metro_region] IS NULL OR
      [parent_metro_region_metro_code] IS NULL
-- No NULLS

--5) Deleting duplicate records in union table ---------------------------------------------------------------

--Checking row counts 
 SELECT COUNT(*)
  FROM [dbo].[county_market_tracker] -- = 925245 rows

 /* Checking whole dataset for duplicates in what should be our unique super key;
(= combination of [region] + [property_type] + [period_end]) */
SELECT [region],[property_type],[period_end], COUNT(*)
  FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
  GROUP BY [region],[property_type],[period_end]
  HAVING COUNT(*) > 1
-- No dups!

--6) Cleaning bad data, converting data types-------------------------------------------------------------------

-- Preview removing excess text from 'region'
SELECT [region],
SUBSTRING([region], 1, CHARINDEX(',', [region])-1) AS County                    
From [dbo].[county_market_tracker]
-- looks good, so update table...

-- Update with fix above
Update [dbo].[county_market_tracker]
SET [region] = SUBSTRING([region], 1, CHARINDEX(',', [region])-1)     
-- (925245 rows affected)

-- Preview data type conversion
SELECT [median_list_price], CONVERT(int,[median_list_price])
  FROM [dbo].[county_market_tracker]
/* Attempt at converting data types threw error: "Conversion failed when converting
the varchar value '1.15e+006' to data type int.", so finding and fixing these entries...*/

-- Preview fix; removing decimals and truncating [median_sale_price] so CONVERT type works
SELECT [median_list_price], 
	SUBSTRING([median_list_price], 1, CHARINDEX('.', [median_list_price])-1) AS Trunc_L_Price,
	CHARINDEX('.', [median_list_price]) AS Decplace
  FROM [dbo].[county_market_tracker]
WHERE [median_list_price] LIKE '%.%'
-- 18,051 rows, 0:02, looks good

-- Updating with fix.
Update [dbo].[county_market_tracker]
SET [median_sale_price] = SUBSTRING([median_sale_price], 1, CHARINDEX('.', [median_sale_price])-1)
WHERE [median_sale_price] LIKE '%.%'
--(17781 rows affected)

-- Updating with fix.
Update [dbo].[county_market_tracker]
SET [median_list_price] = SUBSTRING([median_list_price], 1, CHARINDEX('.', [median_list_price])-1)
WHERE [median_list_price] LIKE '%.%'
-- (18051 rows affected) 

-- Now preview CONVERT data type after bad data fixed
SELECT [median_list_price], CONVERT(int,[median_list_price])
  FROM [dbo].[county_market_tracker]
-- works, so decimal was the problem

--Continuing bulk/batch removal of decimals via ROUND so CONVERT type works
UPDATE  [dbo].[county_market_tracker]
SET [median_ppsf] = ROUND([median_ppsf],0)
,[median_sale_price_mom] = ROUND([median_sale_price_mom],3)
,[median_sale_price_yoy] = ROUND([median_sale_price_yoy],3)
,[median_list_price_mom] = ROUND([median_list_price_mom],3)
,[median_list_price_yoy] = ROUND([median_list_price_yoy],3)
,[median_list_ppsf] = ROUND([median_list_ppsf],0)
,[homes_sold] = ROUND([homes_sold],0)
,[pending_sales] = ROUND([pending_sales],0)
,[new_listings] = ROUND([new_listings],0)
,[inventory] = ROUND([inventory],0)
,[median_dom] = ROUND([median_dom],0)
,[avg_sale_to_list] = ROUND([avg_sale_to_list],3)
,[sold_above_list] = ROUND([sold_above_list],3)
,[off_market_in_two_weeks] = ROUND([off_market_in_two_weeks],3)
  FROM [dbo].[county_market_tracker]
--(925245 rows affected) 0:40

-- Preview removing redundant state text from 'parent-metro_region'
SELECT [parent_metro_region], SUBSTRING([parent_metro_region], 1, 
			CHARINDEX(',', [parent_metro_region])) as preview                    
FROM [dbo].[county_market_tracker]
-- a number of entries are "... nonmetropolitan area", w/o comma

--... finding them...
SELECT [parent_metro_region]                    
FROM [dbo].[county_market_tracker]
WHERE [parent_metro_region] LIKE '%nonmetro%'
-- (237,027 rows)

-- Setting these to null (since there is no parent metro region)
UPDATE [dbo].[county_market_tracker]
SET [parent_metro_region] = NULL                   
WHERE [parent_metro_region] LIKE '%nonmetro%'
--(237027 rows affected)

-- Now preview removing commas and redundant state text from the rest...
SELECT [parent_metro_region], SUBSTRING([parent_metro_region], 1, 
			CHARINDEX(',', [parent_metro_region])-1) as preview                    
FROM [dbo].[county_market_tracker]
-- looks good...

-- ...so updating table
UPDATE [dbo].[county_market_tracker]
SET [parent_metro_region] = SUBSTRING([parent_metro_region], 1, 
							CHARINDEX(',', [parent_metro_region])-1)
-- (925245 rows affected) 0:30

-- Now previewing conversions, after decimals removed from some.
 SELECT 
 [property_type_id], CONVERT(int,[property_type_id]),
 [median_sale_price], CAST([median_sale_price] AS bigint), --CONVERT(int,[median_sale_price]),
 [median_sale_price_mom], CONVERT(decimal(7,3),[median_sale_price]),
 [median_sale_price_yoy], CONVERT(decimal(7,3),[median_sale_price]),
 [median_list_price], CAST([median_list_price] AS bigint),--CONVERT(int,[median_list_price]),
 [median_list_price_mom], CONVERT(decimal(7,3),[median_list_price]),
 [median_list_price_yoy], CONVERT(decimal(7,3),[median_list_price]),
 [median_ppsf], CONVERT(int,[median_ppsf]),
 [median_list_ppsf], CONVERT(int,[median_list_ppsf]),
 [homes_sold], CONVERT(int,[homes_sold]),
 [pending_sales],CONVERT(int,[pending_sales]),
 [new_listings], CONVERT(int,[new_listings]),
 [inventory], CONVERT(int,[inventory]),
 [months_of_supply], CONVERT(decimal(7,3),[months_of_supply]),
 [median_dom], CONVERT(int,[median_dom]),
 [avg_sale_to_list], CONVERT(decimal(7,3),[avg_sale_to_list]),
 [sold_above_list], CONVERT(decimal(7,3),[sold_above_list]),
 [price_drops], CONVERT(decimal(7,3),[price_drops]),
 [off_market_in_two_weeks], CONVERT(decimal(7,3),[off_market_in_two_weeks])
  FROM [dbo].[county_market_tracker]
--"Arithmetic overflow error converting varchar to data type numeric."

-- Looking at range of entries (not really helpful for numbers in varchar form)
SELECT [property_type], 
      MIN(CAST([median_sale_price] AS bigint)) AS MinMedSalePrice,
      MAX(CAST([median_sale_price] AS bigint)) AS MaxMedSalePrice,

	  MIN([median_sale_price_mom]) AS MinMedSalePricemom,
      MAX([median_sale_price_mom]) AS MaxMedSalePricemom,

	  MIN([median_sale_price_yoy]) AS MinMedSalePriceyoy,
      MAX([median_sale_price_yoy]) AS MaxMedSalePriceyoy,
	  
	  MIN(CAST([median_ppsf] AS bigint)) AS Minmedian_ppsf,
      MAX(CAST([median_ppsf] AS bigint)) AS Maxmedian_ppsf,

 	  MIN(CAST([median_list_ppsf] AS bigint)) AS Minmedian_list_ppsf,
      MAX(CAST([median_list_ppsf] AS bigint)) AS Maxmedian_list_ppsf,

 	  MIN(CAST([inventory] AS bigint)) AS Mininventory,
      MAX(CAST([inventory] AS bigint)) AS Maxinventory,

      MIN(CAST([median_list_price] AS bigint)) AS MinMedListPrice,
      MAX(CAST([median_list_price] AS bigint)) AS MaxMedListPrice
FROM [dbo].[county_market_tracker]
--WHERE [median_sale_price] >1 AND [median_sale_price] >1
GROUP BY [property_type]; 
-- looks like we have bad data in all of these.  Will need to clean/trim...

-- Looking at som bad entries...
SELECT [median_sale_price]
FROM [dbo].[county_market_tracker]
WHERE [median_sale_price] = ' ' OR
	  [median_sale_price] < 5000 OR
	  [median_sale_price] > 100000000
ORDER BY [median_sale_price] DESC
-- 862 blanks, 367 < 10000, 1 > 10M

--...declaring them null
UPDATE [dbo].[county_market_tracker]
SET [median_sale_price] = NULL   
WHERE [median_sale_price] = ' ' OR
	  [median_sale_price] < 5000 OR
	  [median_sale_price] > 100000000
-- (1232 rows affected)

-- Looking at some bad entries...
SELECT [median_sale_price_mom], ROUND([median_sale_price_mom],2)
FROM [dbo].[county_market_tracker]
ORDER BY [median_sale_price_mom] DESC

-- rounding before trying to convert type
UPDATE [dbo].[county_market_tracker]
SET [median_sale_price_mom] = ROUND([median_sale_price_mom],2),
    [median_sale_price_yoy] = ROUND([median_sale_price_yoy],2),
    [median_list_price_mom] = ROUND([median_list_price_mom],2),
    [median_list_price_yoy] = ROUND([median_list_price_yoy],2)
--(925245 rows affected)

-- converting type
UPDATE [dbo].[county_market_tracker]
SET [median_sale_price_mom] = CAST([median_sale_price_mom] AS decimal(10,3)),
    [median_sale_price_yoy] = CAST([median_sale_price_yoy] AS decimal(10,3)),
    [median_list_price_mom] = CAST([median_list_price_mom] AS decimal(10,3)),
    [median_list_price_yoy] = CAST([median_list_price_yoy] AS decimal(10,3)),
	[months_of_supply] = CAST([months_of_supply] AS decimal(10,3)),
    [price_drops] = CAST([price_drops] AS decimal(10,3))
--(925245 rows affected) -- BUT IT DIDNT WORK, USING DIFFERENT APPROACH

SELECT [median_list_price_yoy], [months_of_supply], [price_drops]
FROM [dbo].[county_market_tracker]
WHERE [median_list_price_yoy] LIKE '%,%' OR
	[months_of_supply] LIKE '%,%' OR
	[price_drops] LIKE '%,%'
-- None (no commas)

-- Add empty attributes with desired type, to be populated with existing data
ALTER TABLE [dbo].[county_market_tracker]
ADD [property_type_id_c] int,
 [median_sale_price_c] int,
 [median_sale_price_mom_c] decimal(7,3),
 [median_sale_price_yoy_c] decimal(7,3),
 [median_list_price_c] int,
 [median_list_price_mom_c] decimal(7,3),
 [median_list_price_yoy_c] decimal(7,3),
 [median_ppsf_c] int,
 [median_list_ppsf_c] int,
 [homes_sold_c] int,
 [pending_sales_c] int,
 [new_listings_c] int,
 [inventory_c] int,
 [months_of_supply_c] decimal(7,3),
 [median_dom_c] int,
 [avg_sale_to_list_c] decimal(7,3),
 [sold_above_list_c] decimal(7,3),
 [price_drops_c] decimal(7,3),
 [off_market_in_two_weeks_c] decimal(7,3)
 -- Commands completed successfully.

UPDATE [dbo].[county_market_tracker]
SET
 [median_sale_price_c] = CAST([median_sale_price] AS bigint), 
 --[median_sale_price_mom_c] = CONVERT(decimal(7,3),[median_sale_price]),
 --[median_sale_price_yoy_c] = CONVERT(decimal(7,3),[median_sale_price]),
 [median_list_price_c] = CAST([median_list_price] AS bigint),
 --[median_list_price_mom_c] = CONVERT(decimal(7,3),[median_list_price]),
 --[median_list_price_yoy_c] = CONVERT(decimal(7,3),[median_list_price]),
 [median_ppsf_c] = CAST([median_ppsf] AS bigint),
 [median_list_ppsf_c] = CAST([median_list_ppsf] AS bigint)
 --(925245 rows affected) worked w/o mom and yoy attributes - maybe just remove them

 UPDATE [dbo].[county_market_tracker]
SET [homes_sold_c] = CONVERT(int,[homes_sold]),
 [pending_sales_c] = CONVERT(int,[pending_sales]),
 [new_listings_c] = CONVERT(int,[new_listings]),
 [inventory_c] = CONVERT(int,[inventory])
 --(925245 rows affected)

 --remove deciamals from [months_of_supply_c]
SELECT [months_of_supply], 
	SUBSTRING([months_of_supply], 1, CHARINDEX('.', [months_of_supply])-1) AS Trunc_mos
  FROM [dbo].[county_market_tracker]
WHERE [months_of_supply] LIKE '%.%'

UPDATE [dbo].[county_market_tracker] 
SET [months_of_supply] = SUBSTRING([months_of_supply], 1, CHARINDEX('.', [months_of_supply])-1)
WHERE [months_of_supply] LIKE '%.%'
--(662579 rows affected) ...now trying to update again

  UPDATE [dbo].[county_market_tracker]
SET  [months_of_supply_c] = CONVERT(int,[months_of_supply])
 --(925245 rows affected)

 UPDATE [dbo].[county_market_tracker]
SET  [median_dom_c] = CONVERT(int,[median_dom]),
 [avg_sale_to_list_c] = CONVERT(decimal(7,3),[avg_sale_to_list]),
 [sold_above_list_c] = CONVERT(decimal(7,3),[sold_above_list]),
 [off_market_in_two_weeks_c] = CONVERT(decimal(7,3),[off_market_in_two_weeks])
 --(925245 rows affected) 
 
 /* Deleted [price_drops_c], [price_drops],[property_type_id_c],[median_sale_price_mom],[median_sale_price_yoy],
[median_list_price_mom],[median_list_price_yoy]*/

ALTER TABLE [dbo].[county_market_tracker]
DROP COLUMN [property_type_id_c],[median_sale_price_mom],
			[median_sale_price_yoy],[median_list_price_mom],[median_list_price_yoy]
--Commands completed successfully.

/* I came back to redo from this point fwd, because I realized there are a number of
ambiguous county names that exist in multiple states.  So I need DISTINCT [region] [state_code]
*/
SELECT DISTINCT [region] [state_code], [state_code], COUNT(*) AS cnt
FROM [dbo].[county_market_tracker]
WHERE [property_type_id] = '6'
GROUP BY [region], [state_code]
ORDER BY [region], cnt
-- 2933 for US/Ptype 6

-- Looking at all dates for
SELECT DISTINCT [period_end], COUNT(*)
FROM [dbo].[county_market_tracker]
WHERE [property_type_id] = '6'
GROUP BY [period_end]
-- 139 dates for US/Ptype 6

CREATE TABLE FixedUS 
    (enddate date null,
	county VARCHAR(50) null,
	stateabb VARCHAR(50) null,
	medSprice int null,
	FXDmedSprice int null,
	proptypeID int null);
-- Commands completed successfully.

-- Creating new periodend date table for later cross join
CREATE TABLE EndDates
    ([period_end_C] date NULL);
--Commands completed successfully.

-- Populating [period_end_C] from existing
INSERT INTO [dbo].[EndDates] ([period_end_C]) 
	SELECT DISTINCT [period_end] 
	FROM [dbo].[county_market_tracker]
-- (139 rows affected)

-- Using CTE to populate new attributes
WITH FixdUSCTE AS
	(
	SELECT DISTINCT [region] AS cnty, [state_code] AS stabb
	FROM [dbo].[county_market_tracker] 
	WHERE [property_type_id] = '6'
	GROUP BY [region], [state_code]
	)
INSERT INTO [dbo].[FixedUS] (enddate, county, stateabb) 
SELECT [dbo].[EndDates].period_end_C, cnty, stabb
FROM FixdUSCTE
CROSS JOIN [dbo].[EndDates]
-- Worked! 407687 rows affected (=2933*139)

/* I came back here to wipe and re-calculate medSprice to filter out all where [homes_sold] > 3.
Some values in the tableau animation were nutty because only 1 or 2 properties sold.
UPDATE [dbo].[FixedUS]
SET [medSprice] = NULL
--(407687 rows affected)
*/

/*CTE to populate available medSPrices (nice!); have to use UPDATE intead of 
INSERT INTO when populating existing table??*/
WITH FillMSPrice AS
	(SELECT DISTINCT c.[region] AS allcnty, c.[state_code] AS stabb, c.[period_end] AS edate,
			c.[median_sale_price_C] AS MSPrice, c.[property_type_id] AS ptype
	FROM[dbo].[county_market_tracker] c
	WHERE c.[property_type_id] = '6' AND
		  c.[homes_sold] > 3
	GROUP BY c.[region], c.[state_code], c.[period_end], c.[median_sale_price_C], c.[property_type_id]
	)
UPDATE [dbo].[FixedUS]
	SET [medSprice] = MSPrice,
		[proptypeID] = '6'
	FROM FillMSPrice
	RIGHT JOIN [dbo].[FixedUS]
	ON [dbo].[FixedUS].[county] = allcnty AND
	   [dbo].[FixedUS].[stateabb] = stabb AND
	   [dbo].[FixedUS].[enddate] = edate;
--(407687 rows affected)

-- Preview filling in MedSprice NULLS with last know value (for smooth Tableau animation)
SELECT *, 
	LAST_VALUE(medSprice) IGNORE NULLS OVER(PARTITION BY county, stateabb ORDER BY enddate) AS FXDMSPrice
FROM [HousingDataProject_Cnty].[dbo].[FixedUS]
ORDER BY county, stateabb, enddate
-- looks great!

--Populating new attribute with CTE (a good way to join a table to its own data)
WITH FXDMSPrice_CTE AS
(
SELECT [enddate] AS fxdate, [county] AS fxcnty, [stateabb] AS fxst,
		LAST_VALUE(medSprice) IGNORE NULLS OVER(PARTITION BY county, stateabb ORDER BY enddate) AS FXDMSPrice
FROM [HousingDataProject_Cnty].[dbo].[FixedUS]
)
UPDATE [HousingDataProject_Cnty].[dbo].[FixedUS]
	SET [FXDmedSprice] = FXDMSPrice
	FROM FXDMSPrice_CTE
	LEFT JOIN [HousingDataProject_Cnty].[dbo].[FixedUS] fxus
	ON fxus.[county] = fxcnty AND
	   fxus.[stateabb] = fxst AND
	   fxus.[enddate] = fxdate;
--(407687 rows affected)

-- Adding moving average attribute
ALTER TABLE [dbo].[FixedUS]
ADD [SpriceMA] int
--Commands completed successfully.

--Previewing MA of FXDmedSPrice (for prettier animation)
SELECT [enddate], [county], [stateabb], [FXDmedSprice],
	   AVG([FXDmedSprice]) OVER (PARTITION BY [county], [stateabb] 
								 ORDER BY [county], [stateabb], [enddate] 
								 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) ma7
FROM [dbo].[FixedUS]
ORDER BY [county], [stateabb], [enddate]
-- Looks good! Use fwdma5 as basis in pctchg column...

-- Populating MA attribute
WITH MA_CTE AS
(
SELECT [enddate] edate, [county] cnty, [stateabb] stabb, [FXDmedSprice],
	   AVG([FXDmedSprice]) OVER (PARTITION BY [county], [stateabb] 
								 ORDER BY [county], [stateabb], [enddate] 
								 ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) ma7
FROM [dbo].[FixedUS]
)
UPDATE [dbo].[FixedUS]
SET [SpriceMA] = ma7
FROM MA_CTE
JOIN [dbo].[FixedUS] f
ON f.[county] = cnty AND
   f.[stateabb]= stabb AND
   f.[enddate] = edate
-- Warning: Null value is eliminated by an aggregate or other SET operation.
-- (this is telling us nulls are ignored in the AVG calculation; a good thing in this case)
-- (407687 rows affected)

-- Tableau is skipping ambiguous Counties, so making a concatenated version with state code
ALTER TABLE [dbo].[FixedUS]
ADD [FullCounty] varchar(100)
--Commands completed successfully.

--Previewing new 'full county' attribute
SELECT [county], [stateabb], COALESCE([county],'') + ', ' + COALESCE([stateabb],'') AS fullcnty
FROM [dbo].[FixedUS] 

UPDATE [dbo].[FixedUS]
SET [FullCounty] = COALESCE([county],'') + ', ' + COALESCE([stateabb],'')
--(407687 rows affected)

-- Adding percent change attribute
ALTER TABLE [dbo].[FixedUS]
ADD [CumPctChg] decimal(7,2)
--Commands completed successfully.





-- Preview forward moving average to use as basis (one-time initial starting val) for pct chg  

SELECT [enddate], [FullCounty], [FXDmedSprice],
	   AVG([FXDmedSprice]) OVER (PARTITION BY [county], [stateabb] 
								 ORDER BY [county], [stateabb], [enddate] 
								 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) fwdma5
FROM [dbo].[FixedUS]
WHERE [FXDmedSprice] IS NOT NULL
ORDER BY [FullCounty], [enddate]
-- looks good; will join to bring in fwdma5

-- Adding SpriceFwdMA attribute
ALTER TABLE [dbo].[FixedUS]
ADD [SpriceFwdMA] int
--Commands completed successfully.

-- Populate with CTE
WITH FwdMA_CTE AS
(
SELECT [enddate] edate, [FullCounty] fcnty, [FXDmedSprice],
	   AVG([FXDmedSprice]) OVER (PARTITION BY [county], [stateabb] 
								 ORDER BY [county], [stateabb], [enddate] 
								 ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) fwdma5
FROM [dbo].[FixedUS]
WHERE [FXDmedSprice] IS NOT NULL
)
UPDATE [HousingDataProject_Cnty].[dbo].[FixedUS]
	SET [SpriceFwdMA] = fwdma5
	FROM FwdMA_CTE
	LEFT JOIN [HousingDataProject_Cnty].[dbo].[FixedUS] fxus
	ON fxus.[FullCounty] = fcnty AND
	   fxus.[enddate] = edate;
--  (301515 rows affected)

--Previewing cumulative pct Chg (for prettier animation)
SELECT [enddate], [FullCounty], [FXDmedSprice], [SpriceMA], [SpriceFwdMA],
--	   CAST(CAST([SpriceMA] AS FLOAT)/CAST(FIRST_VALUE([SpriceMA]) OVER (PARTITION BY [FullCounty]
--								 ORDER BY [FullCounty], [enddate]) AS FLOAT) AS DECIMAL(7,2)) cumpctchg,
	   CAST(CAST([SpriceMA] AS FLOAT)/CAST(LAG([SpriceMA],3) OVER (PARTITION BY [FullCounty]
								 ORDER BY [FullCounty], [enddate]) AS FLOAT) AS DECIMAL(7,2)) qtrpctchg
FROM [dbo].[FixedUS]
WHERE --[enddate] > '2013-07-31' AND -- this is needed for cumpct chg, but not for qtrpctchg
	  [SpriceMA] IS NOT NULL
ORDER BY [FullCounty], [enddate]
-- Looks ok...

-- Adding QtlyPctChg attribute
ALTER TABLE [dbo].[FixedUS]
ADD [QtrlyPctChg] decimal(7,2)
--Commands completed successfully.


-- Populating Cum/Qtly PctChg attribute using SpriceMA
WITH PctChg_CTE AS
(
SELECT [enddate] edate, [FullCounty] fcnty, [FXDmedSprice], [SpriceMA], [SpriceFwdMA],
--	   CAST(CAST([SpriceMA] AS FLOAT)/CAST(FIRST_VALUE([SpriceMA]) OVER (PARTITION BY [FullCounty]
--								 ORDER BY [FullCounty], [enddate]) AS FLOAT) AS DECIMAL(7,2)) pctchg
	   CAST(CAST([SpriceMA] AS FLOAT)/CAST(LAG([SpriceMA],3) OVER (PARTITION BY [FullCounty]
								 ORDER BY [FullCounty], [enddate]) AS FLOAT) AS DECIMAL(7,2)) qtrpctchg
FROM [dbo].[FixedUS]
WHERE --[enddate] > '2013-07-31' AND -- this is needed for cumpct chg, but not for qtrpctchg
	  [SpriceMA] IS NOT NULL
)
UPDATE [dbo].[FixedUS]
--SET [CumPctChg] = pctchg
SET [QtrlyPctChg] = qtrpctchg
FROM PctChg_CTE
RIGHT JOIN [dbo].[FixedUS] f
ON f.[FullCounty] = fcnty AND
   f.[enddate] = edate
--(407687 rows affected)

-- converting to % change metric
UPDATE [dbo].[FixedUS]
SET [CumPctChg] = [CumPctChg] - 1
WHERE [CumPctChg] IS NOT NULL
--(271253 rows affected)

-- converting to % change metric
UPDATE [dbo].[FixedUS]
SET [QtrlyPctChg] = [QtrlyPctChg] - 1
WHERE [QtrlyPctChg] IS NOT NULL
--(293396 rows affected)

-- averaging all Sprice over a given state and date for a 'line race' chart in tableau
SELECT DISTINCT [enddate], [stateabb], AVG([SpriceMA]) OVER(PARTITION BY [enddate], [stateabb]) AvgSpriceMA
 FROM [HousingDataProject_Cnty].[dbo].[FixedUS]
 WHERE [stateabb] = 'ID' /*OR
		[stateabb] = 'GA' OR
		[stateabb] = 'NJ'  */
 GROUP BY [enddate],[stateabb],[SpriceMA]
 ORDER BY [stateabb],[enddate]
-- nice! Saved as csv for import into Tableau

 -- averaging all SPrices over a given state and date for a 'line race' chart in tableau
SELECT DISTINCT [enddate], [stateabb], AVG([FXDmedSprice]) OVER(PARTITION BY [enddate], [stateabb]) AvPrice
 FROM [HousingDataProject_Cnty].[dbo].[FixedUS]
 /*WHERE [stateabb] = 'ID' OR
		[stateabb] = 'GA' OR
		[stateabb] = 'NJ'  */
 GROUP BY [enddate],[stateabb],[FXDmedSprice]
 ORDER BY [stateabb],[enddate]

 -- Creating new state- and date-aggregated table for tableau line race
CREATE TABLE AggdByState
    ([period_end] date NULL,
	 [stateabb] varchar(50) NULL,
	 [avgSprice] int NULL,
	 [cumpctchg] decimal(7,2) NULL,
	 [qoqpct chg] decimal(7,2) NULL)
--Commands completed successfully.

-- Populating table with all possible state/date combos
WITH FillSt_CTE AS
	(
	SELECT DISTINCT f.[enddate] edate, f.[stateabb] stabb
	FROM [dbo].[FixedUS] f
	GROUP BY f.[enddate], f.[stateabb]
	)
INSERT INTO [dbo].[AggdByState] ([period_end], [stateabb]) 
SELECT edate, stabb
FROM FillSt_CTE
--(6811 rows affected)

-- now populating avgSprice
WITH FillStAvgprice_CTE AS
(
SELECT DISTINCT [enddate] edate, [stateabb] stabb, 
		AVG([FXDmedSprice]) OVER(PARTITION BY [enddate], [stateabb]) AvPrice
 FROM [HousingDataProject_Cnty].[dbo].[FixedUS]
 GROUP BY [enddate],[stateabb],[FXDmedSprice]
 --ORDER BY [stateabb],[enddate]
 )
 UPDATE [dbo].[AggdByState]
SET [avgSprice] = AvPrice
FROM FillStAvgprice_CTE
LEFT JOIN [dbo].[AggdByState] agg
ON agg.[period_end] = edate AND
   agg.[stateabb] = stabb
--Warning: Null value is eliminated by an aggregate or other SET operation.
--(6811 rows affected)

-- Populating Qtrly/CumPctChg attributes using [avgSprice]
WITH PctChgs_CTE AS
(
SELECT [period_end] edate, [stateabb] stabb, [avgSprice] avgp,
	   CAST(CAST([avgSprice] AS FLOAT)/CAST(FIRST_VALUE([avgSprice]) OVER (PARTITION BY [stateabb]
								 ORDER BY [stateabb], [period_end]) AS FLOAT) AS DECIMAL(7,2)) pctchg,
	   CAST(CAST([avgSprice] AS FLOAT)/CAST(LAG([avgSprice],3) OVER (PARTITION BY [stateabb]
								 ORDER BY [stateabb], [period_end]) AS FLOAT) AS DECIMAL(7,2)) qtrpctchg
FROM [dbo].[AggdByState]
WHERE [period_end] > '2013-07-31' AND
	  [avgSprice] IS NOT NULL
)
UPDATE[dbo].[AggdByState] 
SET [cumpctchg] = pctchg,
    [qoqpct chg] = qtrpctchg
FROM PctChgs_CTE
RIGHT JOIN [dbo].[AggdByState] agg
ON agg.[period_end] = edate AND
   agg.[stateabb] = stabb
--(6811 rows affected)

-- converting to % change metric
UPDATE [dbo].[AggdByState]
SET [cumpctchg] = [cumpctchg] - 1
WHERE [cumpctchg] IS NOT NULL
--(5880 rows affected)

-- converting to % change metric
UPDATE [dbo].[AggdByState]
SET [qoqpct chg] = [qoqpct chg] - 1
WHERE [qoqpct chg] IS NOT NULL
--(5733 rows affected)


-- I realized averaging sales prices across date and state wasn't optimal, so this code provides a
-- MUCH BETTER average sale price, weighted by # homes sold. Nested CTEs - nice!!!
WITH wtdAvgSPrice_CTE AS
(
SELECT [period_end] edate, [state_code] stabb, [median_sale_price_c], [homes_sold_c],
	  ROUND([median_sale_price_c]*(CAST([homes_sold_c] AS FLOAT)
					/CAST((SUM([homes_sold_c]) OVER 
								(PARTITION BY [period_end], [state_code])) AS FLOAT)),0) wtdprodXdateST
-- SUM of this variable across date + state IS the average sale price, weighted by # homes sold!!
FROM [HousingDataProject_Cnty].[dbo].[county_market_tracker]
   WHERE [property_type_id] = '6'
--   ORDER BY [state_code], [period_end]
), BettrAvg_CTE AS
(
SELECT edate, stabb, SUM(wtdprodXdateST) OVER(PARTITION BY edate, stabb) bettravg
FROM wtdAvgSPrice_CTE
)
UPDATE [dbo].[AggdByState]
SET [avgSprice] = bettravg
FROM BettrAvg_CTE
RIGHT JOIN [dbo].[AggdByState] agg
ON agg.[period_end] = edate AND
   agg.[stateabb] = stabb
--Warning: Null value is eliminated by an aggregate or other SET operation.
--(6811 rows affected)


-- Re-Populating Qtrly/CumPctChg attributes using better weighted avgSprice...
WITH PctChgs_CTE AS
(
SELECT [period_end] edate, [stateabb] stabb, [avgSprice] avgp,
	   CAST(CAST([avgSprice] AS FLOAT)/CAST(FIRST_VALUE([avgSprice]) OVER (PARTITION BY [stateabb]
								 ORDER BY [stateabb], [period_end]) AS FLOAT) AS DECIMAL(7,2)) pctchg,
	   CAST(CAST([avgSprice] AS FLOAT)/CAST(LAG([avgSprice],3) OVER (PARTITION BY [stateabb]
								 ORDER BY [stateabb], [period_end]) AS FLOAT) AS DECIMAL(7,2)) qtrpctchg
FROM [dbo].[AggdByState]
WHERE [period_end] > '2013-07-31' AND
	  [avgSprice] IS NOT NULL
)
UPDATE[dbo].[AggdByState] 
SET [cumpctchg] = pctchg,
    [qoqpct chg] = qtrpctchg
FROM PctChgs_CTE
RIGHT JOIN [dbo].[AggdByState] agg
ON agg.[period_end] = edate AND
   agg.[stateabb] = stabb
--(6811 rows affected)

-- converting to % change metric
UPDATE [dbo].[AggdByState]
SET [cumpctchg] = [cumpctchg] - 1
WHERE [cumpctchg] IS NOT NULL
--(5880 rows affected)

-- converting to % change metric
UPDATE [dbo].[AggdByState]
SET [qoqpct chg] = [qoqpct chg] - 1
WHERE [qoqpct chg] IS NOT NULL
--(5733 rows affected)

-- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

-- Adding homes sold attribute to [dbo].[FixedUS]
ALTER TABLE [dbo].[FixedUS]
ADD [HomesSold] int NULL
--Commands completed successfully.

-- Populating [HomesSold]
UPDATE [dbo].[FixedUS]
SET [HomesSold] = [homes_sold_c]
FROM [dbo].[county_market_tracker] cm
RIGHT JOIN [dbo].[FixedUS] fus
ON cm.[period_end] = fus.[enddate] AND
   cm.[state_code] = fus.[stateabb] AND
   cm.[region] = fus.[county]
--(407687 rows affected)

-- Replacing NULLs with 0
UPDATE [dbo].[FixedUS]
SET [HomesSold] = '0'
WHERE [HomesSold] IS NULL
-- (95950 rows affected)

-- Preview moving average of Homes Sold for better Tableau animation  
SELECT [enddate], [FullCounty], [HomesSold],
	   AVG([HomesSold]) OVER (PARTITION BY [FullCounty] 
								 ORDER BY [FullCounty], [enddate] 
								 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) ma
FROM [dbo].[FixedUS]
ORDER BY [FullCounty], [enddate]
-- looks good; will join to bring in fwdma5

-- Adding SpriceFwdMA attribute
ALTER TABLE [dbo].[FixedUS]
ADD [HomesSoldMA3] int
--Commands completed successfully.

-- Populate with CTE
WITH HSMA_CTE AS
(
SELECT [enddate] edate, [FullCounty] fcnty, [HomesSold],
	   AVG([HomesSold]) OVER (PARTITION BY [FullCounty] 
								 ORDER BY [FullCounty], [enddate] 
								 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) ma
FROM [dbo].[FixedUS]
)
UPDATE [HousingDataProject_Cnty].[dbo].[FixedUS]
	SET [HomesSoldMA3] = ma
	FROM HSMA_CTE
	RIGHT JOIN [HousingDataProject_Cnty].[dbo].[FixedUS] fxus
	ON fxus.[FullCounty] = fcnty AND
	   fxus.[enddate] = edate;
--  (407687 rows affected)





-- BREAK







-- To fix the error thrown above, previewing those with scientific notation.
SELECT median_ppsf ,median_list_ppsf      
FROM [dbo].[county_market_tracker]
--WHERE median_ppsf LIKE '%e+%' -- (2 rows)
WHERE median_list_ppsf LIKE '%e+%'   -- (2 rows)

-- Replacing scientific notation (huge values that dont make sense for ppsf) with null. 
UPDATE [dbo].[county_market_tracker]
SET median_ppsf = NULL
WHERE median_ppsf LIKE '%e%'
--(2 rows affected)

-- Now the other attribute
UPDATE [dbo].[county_market_tracker]
SET median_list_ppsf = NULL
WHERE median_list_ppsf LIKE '%e%'
--(2 rows affected)

-- Now CONVERT data types after bad data fixed
Update [dbo].[UNION_zip_code_market_tracker]
 SET [median_sale_price] = CONVERT(int,[median_sale_price]),
   [median_list_price] = CONVERT(int,[median_list_price]),
   [median_ppsf] = CONVERT(int,[median_ppsf]),
   [median_list_ppsf] = CONVERT(int,[median_list_ppsf]),
   [homes_sold] = CONVERT(int,[homes_sold]),
   [pending_sales] = CONVERT(int,[pending_sales]),
   [new_listings] = CONVERT(int,[new_listings]),
   [inventory] = CONVERT(int,[inventory]),
   [median_dom] = CONVERT(int,[median_dom]),
   [avg_sale_to_list] = CONVERT(decimal(7,3),[avg_sale_to_list]),
   [sold_above_list] = CONVERT(decimal(7,3),[sold_above_list]),
   [off_market_in_two_weeks] = CONVERT(decimal(7,3),[off_market_in_two_weeks]),
   [last_updated] = CONVERT(date,[last_updated])
--(7177786 rows affected), 2:00 
/* These (above) just ran successfully, but the new data types don't show up in the column view, 
and the table report doesnt show any size change. Apparently requires the approach below...
*/

-- Add empty attributes with desired type, to be populated with existing data
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
ADD [period_end_C] date,
   [median_sale_price_C] int,
   [median_list_price_C] int,
   [median_ppsf_C] int,
   [median_list_ppsf_C] int,
   [homes_sold_C] int,
   [pending_sales_C] int,
   [new_listings_C] int,
   [inventory_C] int,
   [median_dom_C] int,
   [avg_sale_to_list_C] decimal(7,3),
   [sold_above_list_C] decimal(7,3),
   [off_market_in_two_weeks_C] decimal(7,3),
   [last_updated_C] date;
-- Commands completed successfully.

-- Now updating, populating correct-type attributes with existing data from wrong-type attributes
Update [dbo].[UNION_zip_code_market_tracker]
SET [period_end_C] = CONVERT(date,[period_end]),
   [median_sale_price_C] = CONVERT(int,[median_sale_price]),
   [median_list_price_C] = CONVERT(int,[median_list_price]),
   [median_ppsf_C] = CONVERT(int,[median_ppsf]),
   [median_list_ppsf_C] = CONVERT(int,[median_list_ppsf]),
   [homes_sold_C] = CONVERT(int,[homes_sold]),
   [pending_sales_C] = CONVERT(int,[pending_sales]),
   [new_listings_C] = CONVERT(int,[new_listings]),
   [inventory_C] = CONVERT(int,[inventory]),
   [median_dom_C] = CONVERT(int,[median_dom]),
   [avg_sale_to_list_C] = CONVERT(decimal(7,3),[avg_sale_to_list]),
   [sold_above_list_C] = CONVERT(decimal(7,3),[sold_above_list]),
   [off_market_in_two_weeks_C] = CONVERT(decimal(7,3),[off_market_in_two_weeks]),
   [last_updated_C] = CONVERT(date,[last_updated])
-- (7177786 rows affected) 2:00

-- Dropping redundant columns (keeping converted ones, "_C")
 ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
 DROP COLUMN [period_end],
   [median_sale_price],
   [median_list_price],
   [median_ppsf],
   [median_list_ppsf],
   [homes_sold],
   [pending_sales],
   [new_listings],
   [inventory],
   [median_dom],
   [avg_sale_to_list],
   [sold_above_list],
   [off_market_in_two_weeks],
   [last_updated]
-- Commands completed successfully.

-- Checking for NULLs
 SELECT  COUNT(*)
  FROM [HousingDataProject_Cnty].[dbo].[UNION_zip_code_market_tracker]
  WHERE [period_end_C] IS NULL OR
      [region] IS NULL OR
      [state_code] IS NULL OR
      [property_type] IS NULL OR
      [median_sale_price_C] IS NULL OR
      [median_list_price_C] IS NULL OR
      [median_ppsf_C] IS NULL OR
      [median_list_ppsf_C] IS NULL OR
      [homes_sold_C] IS NULL OR
      [pending_sales_C] IS NULL OR
      [new_listings_C] IS NULL OR
      [inventory_C] IS NULL OR
      [median_dom_C] IS NULL OR
      [avg_sale_to_list_C] IS NULL OR
      [sold_above_list_C] IS NULL OR
      [off_market_in_two_weeks_C] IS NULL OR
      [parent_metro_region] IS NULL OR
      [parent_metro_region_metro_code] IS NULL
-- good, no nulls 0:05

--7) Checking stats of [median_sale_price_C], [median_list_price_C]----------------------------------------------

SELECT [property_type], 
      MIN(CAST([median_sale_price_C] AS bigint)) AS MinMedSalePrice,
      MAX(CAST([median_sale_price_C] AS bigint)) AS MaxMedSalePrice,
      AVG(CAST([median_sale_price_C] AS bigint)) AS AvgMedSalePrice,
      MIN(CAST([median_list_price_C] AS bigint)) AS MinMedListPrice,
      MAX(CAST([median_list_price_C] AS bigint)) AS MaxMedListPrice,
      AVG(CAST([median_list_price_C] AS bigint)) AS AvgMedListPrice
FROM [dbo].[UNION_zip_code_market_tracker]
WHERE [median_sale_price_C] >1 AND [median_sale_price_C] >1
GROUP BY [property_type]; 
/* (First had to CAST AS BIGINT to fix 'Arithmetic overflow error') 
Result shows zeros as minimums and 999,999,999 as maximums. Maybe trim outliers.*/

/*Adding price bands to approximate frequency distribution within 
[median_sale_price_C] & [median_list_price_C] */
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
Add [SalePriceBand] varchar(50),
    [ListPriceBand] varchar(50);

-- Populate new attribute
Update [dbo].[UNION_zip_code_market_tracker]
SET [SalePriceBand] = CASE
		WHEN ([median_sale_price_C] > 5000000) THEN 'i) >$5M'
        WHEN ([median_sale_price_C] BETWEEN 2500000 AND 5000000) then 'h) $2.5M - $5M'
        WHEN ([median_sale_price_C] BETWEEN 1000000 AND 2500000) then 'g) $1M - $2.5M'
        WHEN ([median_sale_price_C] BETWEEN 750000 AND 1000000) then 'f) $750K - $1M'
        WHEN ([median_sale_price_C] BETWEEN 500000 AND 750000) then 'e) $500K - $750K'
        WHEN ([median_sale_price_C] BETWEEN 350000 AND 500000) then 'd) $350K - $500K'
        WHEN ([median_sale_price_C] BETWEEN 200000 AND 350000) then 'c) $200K - $350K'
        WHEN ([median_sale_price_C] BETWEEN 100000 AND 200000) then 'b) $100K - $200K'
		ELSE 'a) <$100K'
    END
-- 7177786 rows affected, 01:30

-- Populate new attribute
Update [dbo].[UNION_zip_code_market_tracker]
	SET [ListPriceBand] = CASE
		WHEN ([median_list_price_C] > 5000000) THEN 'i) >$5M'
        WHEN ([median_list_price_C] BETWEEN 2500000 AND 5000000) then 'h) $2.5M - $5M'
        WHEN ([median_list_price_C] BETWEEN 1000000 AND 2500000) then 'g) $1M - $2.5M'
        WHEN ([median_list_price_C] BETWEEN 750000 AND 1000000) then 'f) $750K - $1M'
        WHEN ([median_list_price_C] BETWEEN 500000 AND 750000) then 'e) $500K - $750K'
        WHEN ([median_list_price_C] BETWEEN 350000 AND 500000) then 'd) $350K - $500K'
        WHEN ([median_list_price_C] BETWEEN 200000 AND 350000) then 'c) $200K - $350K'
        WHEN ([median_list_price_C] BETWEEN 100000 AND 200000) then 'b) $100K - $200K'
		ELSE 'a) <$100K'
    END
-- 7177786 rows affected ~2:00

-- Preview outliers / sale prices that are out-of-range or dont make sense 
SELECT * 
  FROM [dbo].[UNION_zip_code_market_tracker]
	WHERE [median_sale_price_C] = 0  AND
	      [median_list_price_C] = 0

--Removing 
DELETE
  FROM [dbo].[UNION_zip_code_market_tracker]
	WHERE [median_sale_price_C] = 0  AND
	      [median_list_price_C] = 0
-- 873 rows deleted

-- Preview outliers / sale prices that are out-of-range or dont make sense 
SELECT *
  FROM [dbo].[UNION_zip_code_market_tracker]
	WHERE [median_sale_price_C] = 0

-- Removing
DELETE --SELECT *
  FROM [dbo].[UNION_zip_code_market_tracker]
	WHERE [median_sale_price_C] = 0
-- 12839 rows deleted

-- Preview outliers / sale prices that are out-of-range or dont make sense 
SELECT *
  FROM [dbo].[UNION_zip_code_market_tracker]
	WHERE [median_sale_price_C] < 10000 OR
		  [median_sale_price_C] > 500000000
	ORDER BY [median_sale_price_C] DESC

-- Removing
DELETE
  FROM [dbo].[UNION_zip_code_market_tracker]
	WHERE [median_sale_price_C] < 10000 OR
		  [median_sale_price_C] > 500000000
-- 13681 rows deleted

-- Look at count of sales by [property_type] and [SalePriceBand]
SELECT  [property_type], [SalePriceBand], 
	Count([SalePriceBand]) As CountofSales
FROM [dbo].[UNION_zip_code_market_tracker]
	GROUP BY [property_type], [SalePriceBand]
	ORDER BY [SalePriceBand]

-- Look count of sales by [property_type], [state_code] and ave [median_sale_price_C]
SELECT  DISTINCT [property_type], [state_code],
    AVG(CAST([median_sale_price_C] AS bigint)) OVER (PARTITION BY [state_code]) AS AvgSalePrice
FROM [dbo].[UNION_zip_code_market_tracker]
	GROUP BY [property_type], [state_code], [median_sale_price_C]
	ORDER BY [property_type]
/* Must GROUP BY all the columns in SELECT, or get error "8120; 
Column '_' is invalid in the select list because it is not contained in either
an aggregate function or the GROUP BY clause." Must CAST AS bigint 
or get 'arithmetic overflow error' */

/*
8) Normalizing database----------------------------------------------------------------------------------------
1NF: 
Done! a) Each attribute (cell) contains a single value (no sets of or nested values; "atomicity"=1)
Done! b) No repeating groups in attributes (ex. class1/class2/class3 in each student record)

2NF: 
Done! a) Must be in 1NF.
Pending (see below)  b) Every non-prime attribute (ex. store address) has a full functional
dependency on the whole of every candidate key (ex. if candidate key = cust_id + store_id, then 
store_address must be split from cust_id into its own table with store_id).
*/ 

-- checking data for completeness by date
SELECT  DISTINCT [period_end_C], COUNT([period_end_C])
FROM [dbo].[UNION_zip_code_market_tracker]
	GROUP BY [period_end_C]
	ORDER BY 2 DESC
-- 136 unique dates, but varying counts for each (from 41k-61k)

-- checking data for completeness by zipcode ([region])
SELECT  DISTINCT [region], COUNT([region]) AS ZipCount
FROM [dbo].[UNION_zip_code_market_tracker]
	GROUP BY [region]
	ORDER BY 2 DESC
-- 23,688 unique zip codes, but varying counts for each (from 2-680);

-- Looking at number of entries for a certain [property_type] by zip code
SELECT  DISTINCT [region], COUNT([region]) AS ZipCount, [property_type]
FROM [dbo].[UNION_zip_code_market_tracker]
--WHERE [property_type] = 'Townhouse'
	GROUP BY [region], [property_type]
	ORDER BY 3, 2 DESC
-- results seem plausible

-- looking for outliers using z-score (needs attributes converted to integer inputs)
SELECT median_sale_price_C, [property_type],
	(median_sale_price_C-AVG(CAST([median_sale_price_C] AS bigint)) Over())/
	(STDEV(CAST([median_sale_price_C] AS bigint)) Over()) AS Zscore
  FROM [dbo].[UNION_zip_code_market_tracker]
ORDER BY 3 DESC
/* Nice, but this needs tweaking; this only makes sense if an average is obtained
for each property type, i.e. z-score should be specific to property type. Re-address
after normalization... */

/* Downloaded 30-year mortgage rate data from FRED anticipating it may be useful to
have in tableau.  Formatting and linking it to existing dataset.  Chaged data types
during import to 'date' and 'decimal(7,2)'

I used functions in Excel to quickly fix the mismatched dates between tables; 
([dbo].[UNION_zip_code_market_tracker][period_end_C] is the last day of every month, 
whereas [dbo].[MORTGAGE30US$].[Date_fmttd] is the last business day of each week
subject to holidays. For this project, the mortgage rate on record 1-6 days nearest 
the eom is sufficient. */

-- Looking at how many [period_end_C] from 'UNION' table have matches in 'MORTGAGE' table 
SELECT  DISTINCT(u.period_end_C), m.[FIXD Date_fmttd]
  FROM [dbo].[UNION_zip_code_market_tracker] u
JOIN [dbo].[MORTGAGE30US$] m ON u.period_end_C = m.[FIXD Date_fmttd]
-- Good, 136 out of 136 distict [period_end_C]

-- Adding attribute for rate data to union table
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
Add [30yr_mortrt] decimal(7,2)
-- Commands completed successfully.

-- Now insert mortgege rate into UNION table
-- DONT RUN THIS    UPDATE [dbo].[UNION_zip_code_market_tracker]
	SET [30yr_mortrt] = m.[MORTGAGE30US]
	FROM [dbo].[MORTGAGE30US$] m
	LEFT JOIN [dbo].[UNION_zip_code_market_tracker] u
	ON m.[FIXD_Date_fmttd] = u.[period_end_C];
/* WAIT - WHY? Cancelled query; since I'm essentially writing 136 rates (1 per distinct date) ~7M times,
I'll find a better way - probably just set a rates by date table during 2NF normalization. */

--9) Creating new tables as part of normalization---------------------------------------------------------------

-- Creating new mortgage rate table and setting its primary key
CREATE TABLE MortRates 
    ([period_end_C] DATE PRIMARY KEY CLUSTERED,
	[30yr_mortrt] DECIMAL(7,2));
--Commands completed successfully.

-- Populating [period_end_C] from existing
INSERT INTO [dbo].[MortRates] ([period_end_C]) 
	SELECT DISTINCT [period_end_C] 
	FROM [dbo].[UNION_zip_code_market_tracker];
-- (136 rows affected)

-- Populating [30yr_mortrt] from existing
UPDATE [dbo].[MortRates]
	SET [30yr_mortrt] = r.[FIXD Rt]
	FROM [dbo].[MortRates] m
	RIGHT JOIN [dbo].[MORTGAGE30US$] r
	ON m.[period_end_C] = r.[FIXD_Date_fmttd];
--(136 rows affected)

-- Dropping redundant mortgage table (source still be available elsewhere if needed)
DROP TABLE [dbo].[MORTGAGE30US$];
--Commands completed successfully.

-- Creating new zip code table and setting its primary key
CREATE TABLE ZipCodes 
    (zip VARCHAR(50) PRIMARY KEY CLUSTERED,
	usstate VARCHAR(50),
	metro VARCHAR(50),
	metrozip VARCHAR(50));
-- Commands completed successfully.

-- Populating zip from existing
INSERT INTO [dbo].[ZipCodes] ([zip]) 
	SELECT DISTINCT [region] 
	FROM [dbo].[UNION_zip_code_market_tracker];
-- (23688 rows affected)

-- Populating other attributes from existing
UPDATE [dbo].[ZipCodes]
	SET [usstate] = u.[state_code],
		[metro] = u.[parent_metro_region],
		[metrozip] = u.[parent_metro_region_metro_code]
	FROM [dbo].[ZipCodes] z
	RIGHT JOIN [dbo].[UNION_zip_code_market_tracker] u
	ON z.[zip] = u.[region];
-- (23688 rows affected), 0:02

-- Creating new property type table and setting its primary key
CREATE TABLE PropType 
    (id int PRIMARY KEY CLUSTERED,
	proptype VARCHAR(50));
-- Commands completed successfully.

-- Looking at descending order of property type by count 
SELECT DISTINCT [property_type], COUNT([property_type]),
	   DENSE_RANK() OVER (ORDER BY COUNT([property_type]) DESC) AS RowNo
FROM [dbo].[UNION_zip_code_market_tracker]
GROUP BY [property_type]

-- Using CTE to populate new attributes
WITH ProptypeCTE AS
	(
	SELECT DISTINCT [property_type] AS ptypes,
		DENSE_RANK() OVER (ORDER BY COUNT([property_type]) DESC) AS rowno
		FROM [dbo].[UNION_zip_code_market_tracker]
		GROUP BY [property_type]
	)
INSERT INTO [dbo].[PropType] ([id], [proptype]) 
SELECT rowno, ptypes
FROM ProptypeCTE
-- (5 rows affected)

-- Dropping attributes from union table that are now redundant b/c 
-- they're in other tables, or are no longer useful ([last_updated_C])
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
 DROP COLUMN [state_code],
	[parent_metro_region],
	[parent_metro_region_metro_code],
	[last_updated_C],
	[30yr_mortrt];
-- Commands completed successfully

-- Creating new SalePriceBand table and setting its primary key
CREATE TABLE SPriceBand 
    (id int PRIMARY KEY CLUSTERED,
	spriceband VARCHAR(50));
-- Commands completed successfully.

-- Looking at ascending order of band (helps that I labeled them alphabetically)
SELECT DISTINCT [SalePriceBand] AS spband,
	   DENSE_RANK() OVER (ORDER BY [SalePriceBand]) AS sprank
FROM [dbo].[UNION_zip_code_market_tracker]
GROUP BY [SalePriceBand]

-- Using CTE to populate new attributes
WITH SPBandCTE AS
	(
	SELECT DISTINCT [SalePriceBand] AS spband,
	   DENSE_RANK() OVER (ORDER BY [SalePriceBand]) AS sprank
	FROM [dbo].[UNION_zip_code_market_tracker]
	GROUP BY [SalePriceBand]
	)
INSERT INTO [dbo].[SPriceBand] ([id], [spriceband]) 
SELECT sprank, spband
FROM SPBandCTE
-- (9 rows affected)

-- Creating new ListPriceBand table and setting its primary key
CREATE TABLE LPriceBand 
    (id int PRIMARY KEY CLUSTERED,
	lpriceband VARCHAR(50));
-- Commands completed successfully.

-- Looking at ascending order of band (helps that I labeled them alphabetically)
SELECT DISTINCT [ListPriceBand] AS lpband,
	   DENSE_RANK() OVER (ORDER BY [ListPriceBand]) AS lprank
FROM [dbo].[UNION_zip_code_market_tracker]
GROUP BY [ListPriceBand]

-- Using CTE to populate new attributes
WITH LPBandCTE AS
	(
	SELECT DISTINCT [ListPriceBand] AS lpband,
	   DENSE_RANK() OVER (ORDER BY [ListPriceBand]) AS lprank
	FROM [dbo].[UNION_zip_code_market_tracker]
	GROUP BY [ListPriceBand]
	)
INSERT INTO [dbo].[LPriceBand] ([id], [lpriceband]) 
SELECT lprank, lpband
FROM LPBandCTE
-- (9 rows affected)

-- Changing null rule for attribute for 1/3 of the composite key
ALTER TABLE [dbo].[UNION_zip_code_market_tracker] 
ALTER COLUMN [period_end_C] date NOT NULL
--Commands completed successfully. ~2:30

-- Changing null rule for attribute for 2/3 of the composite key
ALTER TABLE [dbo].[UNION_zip_code_market_tracker] 
ALTER COLUMN [region] varchar(50) NOT NULL
--Commands completed successfully ~3:00

-- Changing null rule for attribute for 3/3 of the composite key
ALTER TABLE [dbo].[UNION_zip_code_market_tracker] 
ALTER COLUMN [property_type] varchar(50) NOT NULL
--Commands completed successfully ~3:00

-- Now declaring composite primary key in union table (design method times out)
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
	ADD CONSTRAINT PK_myConstraint 
PRIMARY KEY ([period_end_C],[region],[property_type])
-- Commands completed successfully

/*
renamed union table attributes as follows:
[property_type] to [property_type_id]
[SalePriceBand] to [SalePriceBand_id]
[ListPriceBand] to [ListPriceBand_id]

renamed PropType table attributes as follows:
[id] to [proptype_id]
Changed LPriceBand table attributes as follows:
[id] to [lpriceband_id]
Changed SPriceBand table attributes as follows:
[id] to [spriceband_id]
*/

-- Replacing text-filled attributes with ids
Update [dbo].[UNION_zip_code_market_tracker]
	SET [ListPriceBand_id] = CASE
		WHEN ([median_list_price_C] > 5000000) THEN '9'
        WHEN ([median_list_price_C] BETWEEN 2500000 AND 5000000) then '8'
        WHEN ([median_list_price_C] BETWEEN 1000000 AND 2500000) then '7'
        WHEN ([median_list_price_C] BETWEEN 750000 AND 1000000) then '6'
        WHEN ([median_list_price_C] BETWEEN 500000 AND 750000) then '5'
        WHEN ([median_list_price_C] BETWEEN 350000 AND 500000) then '4'
        WHEN ([median_list_price_C] BETWEEN 200000 AND 350000) then '3'
        WHEN ([median_list_price_C] BETWEEN 100000 AND 200000) then '2'
		ELSE '1'
    END
-- (7150393 rows affected) 01:00

-- Replacing text-filled attributes with ids
Update [dbo].[UNION_zip_code_market_tracker]
	SET [SalePriceBand_id] = CASE
		WHEN ([median_sale_price_C] > 5000000) THEN '9'
        WHEN ([median_sale_price_C] BETWEEN 2500000 AND 5000000) then '8'
        WHEN ([median_sale_price_C] BETWEEN 1000000 AND 2500000) then '7'
        WHEN ([median_sale_price_C] BETWEEN 750000 AND 1000000) then '6'
        WHEN ([median_sale_price_C] BETWEEN 500000 AND 750000) then '5'
        WHEN ([median_sale_price_C] BETWEEN 350000 AND 500000) then '4'
        WHEN ([median_sale_price_C] BETWEEN 200000 AND 350000) then '3'
        WHEN ([median_sale_price_C] BETWEEN 100000 AND 200000) then '2'
		ELSE '1'
    END
-- (7150393 rows affected)  01:00

-- Replacing text-filled attributes with ids
Update [dbo].[UNION_zip_code_market_tracker]
	SET [property_type_id] = CASE
		WHEN ([property_type_id] = 'All Residential') THEN '1'
		WHEN ([property_type_id] = 'Single Family Residential') THEN '2'
		WHEN ([property_type_id] = 'Condo/Co-op') THEN '3'
		WHEN ([property_type_id] = 'Townhouse') THEN '4'
		ELSE '5'
    END
-- (7150393 rows affected) 4:00

-- Resetting data type for consistency with its connection 
ALTER TABLE [dbo].[UNION_zip_code_market_tracker] 
ALTER COLUMN [SalePriceBand_id] int
--Commands completed successfully. 01:00

-- Resetting data type for consistency with its connection 
ALTER TABLE [dbo].[UNION_zip_code_market_tracker] 
ALTER COLUMN [ListPriceBand_id] int
--Commands completed successfully. 01:00

-- Resetting data type for consistency with its connection 
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
ALTER COLUMN [property_type_id] int NOT NULL
/*The object 'PK_myConstraint' is dependent on column 'property_type_id'.
ALTER TABLE ALTER COLUMN property_type_id failed because one or more objects access this column.*/

-- Have to DROP CONSTRAINT named in error msg above before changing datatype
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
	DROP CONSTRAINT PK_myConstraint
--Commands completed successfully.

-- Now changing data type
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
ALTER COLUMN [property_type_id] int NOT NULL
--Commands completed successfully.

-- ADD CONSTRAINT back, though renamed and has to be as a composite PK
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
	ADD CONSTRAINT PK_unioncompPK_constraint 
PRIMARY KEY ([region],[property_type_id],[period_end_C])
-- Commands completed successfully

/* I'll forego replacing [region] in union table with a unique ID, as the ID would be 
similar in size to the original data ~24k unique, and repeated just as often since the
data is organized by time series ([period_end_C]). But -- I do think we'd benefit from
replacing [period_end_C] with an ID since there are only 136 unique values and they consume
more space than a 3-digit integer (1-136)... */

-- Creating new PeriodEndDate table and setting its primary key
CREATE TABLE PEndDate 
    (pdate_id int PRIMARY KEY CLUSTERED,
	periodenddate date);
-- Commands completed successfully.

-- Looking at ascending order of date
SELECT DISTINCT [period_end_C] AS pend,
	   DENSE_RANK() OVER (ORDER BY [period_end_C]) AS pendrank
FROM [dbo].[UNION_zip_code_market_tracker]
GROUP BY [period_end_C]

-- Using CTE to populate new attributes
WITH PrdEndCTE AS
	(
	SELECT DISTINCT [period_end_C] AS pend,
	   DENSE_RANK() OVER (ORDER BY [period_end_C]) AS pendrank
	FROM [dbo].[UNION_zip_code_market_tracker]
	GROUP BY [period_end_C]
	)
INSERT INTO [dbo].[PEndDate] ([pdate_id],[periodenddate]) 
SELECT pendrank, pend
FROM PrdEndCTE
--(136 rows affected) 0:00

/* Adding a new column in union to replace current [period_end_C]; has to be null
for now, since not populated yet.  To be adjusted later */
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
ADD [period_end_id] int NULL
--Commands completed successfully.

-- Populating new attribute with ids based on matching dates
UPDATE [dbo].[UNION_zip_code_market_tracker]
	SET [period_end_id] = p.[pdate_id]
	FROM [dbo].[PEndDate] p
	LEFT JOIN [dbo].[UNION_zip_code_market_tracker] u
	ON p.[periodenddate] = u.[period_end_C];
--(7150393 rows affected) 02:00

-- Now dropping CONSTRAINT before dropping [period_end_C]
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
	DROP CONSTRAINT PK_unioncompPK_constraint
--Commands completed successfully.

-- DROPPING redundant attribute
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
DROP COLUMN [period_end_C]
--Commands completed successfully.

-- Making new [period_end_id] not nullable, so it can be a part of composite PK, below
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
ALTER COLUMN [period_end_id] int NOT NULL
--Commands completed successfully.

-- Set new composite PK and ADD CONSTRAINT, with new and attributes
ALTER TABLE [dbo].[UNION_zip_code_market_tracker]
	ADD CONSTRAINT PK_unioncompPK_constraint 
PRIMARY KEY ([region],[property_type_id],[period_end_id])
-- Commands completed successfully

/* 10) Normalization summary ---------------------------------------------------------------------------------------

1NF: 
Done! a) Each attribute (cell) contains a single value (no sets of or nested values; "atomicity"=1)
Done! b) No repeating groups in attributes (ex. class1/class2/class3 in each student record)

2NF: 
Done! a) Must be in 1NF.
Done! b) Every non-prime attribute (ex. store address) has a full functional
dependency on the whole of every candidate key (ex. if candidate key = cust_id + store_id, then 
store_address must be split from cust_id into its own table with store_id).

3NF: 
Done! a) Must be in 2NF.
Done! b) Every non-tivial functional dependency either begins with a superkey or ends with a prime
attribute (each attribute depends only on candidate/composite key). For example, the non-key 'product
origin' which describes the 'product manufacturer' which describes the primary key 'product_id' isn't 
in 3NF until 'product origin' is broken into its own table. */ 

--12) Finished by building database structure and relationships (see database diagram)------------------------------









/* Retrieve attributes from multiple tables, given multiple joins and conditions
TX, AllResidential zip codes with less-than-full date representation (< 136 dates) 
WHERE z.[usstate] = 'TX'
AND u.[property_type_id] = '2'*/

SELECT *
FROM (
SELECT DISTINCT u.[region], COUNT(*) AS ZipCount
FROM [dbo].[UNION_zip_code_market_tracker] u
	JOIN [dbo].[ZipCodes] z
	ON u.[region] = z.[zip]
WHERE u.[property_type_id] = '2'
GROUP BY u.[region]
--ORDER BY 2
) sub
WHERE ZipCount <136
-- 13,438 for US/Ptype 2

CREATE TABLE FixedUS 
    (enddate date null,
	zip VARCHAR(50) null,
	medSprice VARCHAR(50) null,
	FXDmedSprice VARCHAR(50) null,
	proptypeID int null);
-- Commands completed successfully.

-- Looking at all zip codes for
SELECT DISTINCT u.[region], COUNT(*)
FROM [dbo].[UNION_zip_code_market_tracker] u
	JOIN [dbo].[ZipCodes] z
	ON u.[region] = z.[zip]
WHERE u.[property_type_id] = '2'
GROUP BY u.[region]
-- 23,442 for US/Ptype 2

-- Using CTE to populate new attributes
WITH FixdUSCTE AS
	(
	SELECT DISTINCT u.[region] AS USzips
	FROM [dbo].[UNION_zip_code_market_tracker] u
	JOIN [dbo].[ZipCodes] z
	ON u.[region] = z.[zip]
	WHERE u.[property_type_id] = '2'
	GROUP BY u.[region]
	)
INSERT INTO [dbo].[FixedUS] ([zip],[enddate]) 
SELECT USzips, [dbo].[PEndDate].periodenddate
FROM FixdUSCTE
CROSS JOIN [dbo].[PEndDate]
-- Worked! Created 3,188,112 rows (=136 distinct dates x 23,442 zip codes) 0:16

/*CTE to populate available medSPrices (nice!); have to use UPDATE intead of 
INSERT INTO when populating existing table??*/
WITH FillMSPrice AS
	(SELECT DISTINCT u.[region] AS allzip, p.[periodenddate] AS edate,
			u.[median_sale_price_C] AS MSPrice, u.[property_type_id] AS ptype
	FROM [dbo].[UNION_zip_code_market_tracker] u
	JOIN [dbo].[PEndDate] p
	ON u.[period_end_id] = p.[pdate_id]
	WHERE u.[property_type_id] = '2'
	GROUP BY u.[region], p.[periodenddate], u.[median_sale_price_C], u.[property_type_id]
	)
UPDATE [dbo].[FixedUS]
	SET [medSprice] = MSPrice
	FROM FillMSPrice
	RIGHT JOIN [dbo].[FixedUS]
	ON [dbo].[FixedUS].[zip] = allzip AND
	   [dbo].[FixedUS].[enddate] = edate;
--(3188112 rows affected)

-- Preview filling in MedSprice NULLS with last know value (for smooth Tableau animation)
SELECT *, 
	LAST_VALUE(medSprice) IGNORE NULLS OVER(PARTITION BY zip ORDER BY enddate) AS FXDMSPrice
FROM [HousingDataProject_Cnty].[dbo].[FixedUS]
ORDER BY zip, enddate
-- looks great!

--Populating new attribute with CTE
WITH FXDMSPrice_CTE AS
(
SELECT [enddate] AS fxdate, [zip] AS fxzip, LAST_VALUE(medSprice) IGNORE NULLS 
		OVER(PARTITION BY zip ORDER BY enddate) AS fxdp
FROM [HousingDataProject_Cnty].[dbo].[FixedUS]
)
UPDATE [HousingDataProject_Cnty].[dbo].[FixedUS]
	SET [FXDmedSprice] = fxdp
	FROM FXDMSPrice_CTE
	LEFT JOIN [HousingDataProject_Cnty].[dbo].[FixedUS] fxus
	ON fxus.[zip] = fxzip AND
	   fxus.[enddate] = fxdate;
--(3188112 rows affected)










