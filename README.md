# ETL & Normalization of Housing Market Data

Transforming raw housing market data into a cleaned and normalized relational database, start to finish.

## Description
The enclosed file (HousingProject_Cnty.sql) represents all of the work performed to turn raw housing market data into a cleaned and normalized relational database for later visualization in Tableau.
The purpose of this project was to:
1)	get more acquainted with SQL Server Management Studio (SSMS),
2)	develop my understanding of SQL/T-SQL,
3)	clean and transform the data for later visualization in Tableau, and
4)	finish with a normalized database (at least 3rd normal form).

The source data [file](https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz) was ~7.1 million rows of raw housing data (courtesy of Redfin, a national real estate brokerage). After extracting/loading into SSMS, I proceeded to poke around with a series of exploratory queries. I identified what data to keep/drop, and performed several iterations of cleaning, reshaping, and data type conversions before some initial statistical analysis.  Finally, I split the data into tables and structured into an efficient relational database in 3rd normal form. There are several examples of CTEs (Common Table Expressions) and nested CTEs that can be copied for use elsewhere.  

### Dependencies

* SQL Server Management Studio version 19.1.56 or later
* Windows 10 or later
