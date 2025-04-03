-- SQL Dupe Removal

-- This process will allow you to remove duplicates from a table on a one-off basis.

-- Step 1 - Create a backup

-- Depending on the size of the table, you may only want to back up select data.

CREATE TABLE [temptablename] AS
SELECT *
FROM [tablename]
WHERE date_value > [required date] -- Filter for dates greater than the specified date.
AND date_value < [required date]; -- Filter for dates less than the specified date.
COMMIT; -- Commit the creation of the backup table.

-- Step 2 - Temp table to identify duplicates

CREATE TABLE wrk.madtest AS
SELECT row_Num, [All columns]
FROM (
    SELECT 
        row_number() OVER (PARTITION BY [list columns for distinct rows] ORDER BY [column name]) AS row_Num, -- Assign row numbers to identify duplicates based on specified columns.
        [All columns] -- Select all columns from the source table.
    FROM [source table]
    WHERE [date range] -- Filter the source table based on the specified date range.
    GROUP BY [All columns] -- Group by all columns to ensure distinct rows are considered for row numbering.
)
WHERE row_Num >= 1; -- Select all rows, including the first occurrence of each duplicate.

-- Step 3 - Remove data from original table

DELETE FROM [source table]
WHERE [date range]; -- Delete data from the original table based on the specified date range.

-- Step 4 - Remove duplicates from temp table

DELETE FROM wrk.madtest
WHERE row_Num > 1; -- Delete all rows with row_Num greater than 1, leaving only the first occurrence of each distinct row.

-- Step 5 - Insert de-duplicated data

INSERT INTO [source table]