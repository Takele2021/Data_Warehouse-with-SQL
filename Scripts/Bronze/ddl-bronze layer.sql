/*
================================================================================
Stored Procedure : bronze.load_bronze
Purpose          : Load CSV files into Bronze schema (Medallion Architecture)
Author           : <Your Name>
Created On       : <Date>
================================================================================

Description:
    This stored procedure loads raw data from CSV files into the Bronze schema.
    It performs the following actions:
      • Truncates target Bronze tables
      • Loads CSV files using BULK INSERT
      • Logs duration for each table
      • Handles all errors using TRY/CATCH

Best Practices Applied:
      ✔ Centralized file locations
      ✔ Reusable BULK LOAD logic
      ✔ Consistent logging format
      ✔ Industry-standard TRY/CATCH block
      ✔ Clearly documented sections

Usage Example:
      EXEC bronze.load_bronze;
================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

   /* -----------------------------------------------------------------
    -- DECLARATIONS
    -----------------------------------------------------------------
*/
    DECLARE
        @batch_start DATETIME = GETDATE(),
        @tbl SYSNAME,           -- Table name
        @file NVARCHAR(4000),   -- File Path
        @start_time DATETIME,
        @end_time DATETIME,
        @sql NVARCHAR(MAX);
        -- @file_exists BIT is no longer needed

    PRINT '============================================================';
    PRINT 'Starting Bronze Layer Load Process';
    PRINT '============================================================';

   /*
-----------------------------------------------------------------
    -- 1. DEFINE LOAD LIST
    -----------------------------------------------------------------
*/
    DECLARE @LoadList TABLE
    (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        TableName SYSNAME,
        FilePath NVARCHAR(4000)
    );

    INSERT INTO @LoadList (TableName, FilePath)
    VALUES
        ('bronze.crm_cust_info',      'C:\Users\takele\Documents\csv-files\bronze.crm_cust_info.csv'),
        ('bronze.crm_prd_info',       'C:\Users\takele\Documents\csv-files\bronze.crm_prd_info.csv'),
        ('bronze.crm_sales_details',  'C:\Users\takele\Documents\csv-files\bronze.crm_sales_details.csv'),
        ('bronze.erp_loc_a101',       'C:\Users\takele\Documents\csv-files\bronze.erp_loc_a101.csv'),
        ('bronze.erp_cust_az12',      'C:\Users\takele\Documents\csv-files\bronze.erp_cust_az12.csv'),
        ('bronze.erp_px_cat_g1v2',    'C:\Users\takele\Documents\csv-files\bronze.erp_px_cat_g1v2.csv');

    /*
    -----------------------------------------------------------------
    -- 2. ITERATE AND LOAD
    -----------------------------------------------------------------
*/
    DECLARE
        @i INT = 1,
        @max INT = (SELECT COUNT(*) FROM @LoadList);

    WHILE @i <= @max
    BEGIN
        SELECT @tbl = TableName, @file = FilePath
        FROM @LoadList WHERE ID = @i;

        PRINT '------------------------------------------------------------';
        PRINT 'Processing Table: ' + @tbl;
        PRINT 'File: ' + @file;

        SET @start_time = GETDATE();

        BEGIN TRY

            /*
            -------------------------------------------------------
            -- Step 1: TRUNCATE TABLE
            -------------------------------------------------------
        */
            SET @sql = N'TRUNCATE TABLE ' + @tbl + N';';
            EXEC sys.sp_executesql @sql;
/*
            -------------------------------------------------------
            -- Step 2: BULK INSERT (FIXED: Embedding file path)
            -- If the file is missing or permissions are wrong, this step will fail 
            -- and immediately jump to the CATCH block below.
            -------------------------------------------------------
*/
            SET @sql = N'
                BULK INSERT ' + @tbl + N'
                FROM ''' + @file + N''' 
                WITH
                (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = '','',
                    ROWTERMINATOR = ''\n'',
                    TABLOCK
                );';

            EXEC sys.sp_executesql @sql;

            -------------------------------------------------------
            -- Step 3: Logging
            -------------------------------------------------------
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';
            PRINT 'OK';

        END TRY
        BEGIN CATCH
            -- The CATCH block handles all errors (including file not found errors from BULK INSERT)
            PRINT '*** ERROR loading table ' + @tbl + ' ***';
            PRINT 'Message: ' + ERROR_MESSAGE();
            PRINT 'Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        END CATCH;

        -- No GOTO needed, just increment the counter for the next loop iteration
        SET @i += 1;
    END

    PRINT '============================================================';
    PRINT 'Bronze Layer Load Completed.';
    PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, GETDATE()) AS NVARCHAR) + ' seconds';
    PRINT '============================================================';

END
GO
