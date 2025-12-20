/*==============================================================================
PROCEDURE NAME: silver.load_silver
================================================================================
PURPOSE:
    This stored procedure performs ETL (Extract, Transform, Load) operations 
    to load the Silver layer of the data warehouse from Bronze layer sources.
    The Silver layer applies data quality rules, standardization, and business
    logic transformations to create cleaned, conformed data.

BUSINESS LOGIC:
    - Deduplicates records based on latest create date
    - Standardizes reference data (gender, marital status, product lines)
    - Handles null values and invalid data
    - Derives missing values using business rules
    - Normalizes data formats and types
    - Calculates derived fields (end dates, sales amounts)

SOURCE TABLES:
    - bronze.crm_cust_info      : Customer master data from CRM
    - bronze.crm_prd_info       : Product information from CRM
    - bronze.crm_sales_details  : Sales transaction details from CRM
    - bronze.erp_px_cat_g1v2    : Product category data from ERP
    - bronze.erp_cust_az12      : Customer demographics from ERP
    - bronze.erp_loc_a101       : Customer location data from ERP

TARGET TABLES:
    - silver.crm_cust_info      : Cleaned customer information
    - silver.crm_prd_info       : Standardized product information
    - silver.crm_sales_details  : Validated sales transactions
    - silver.erp_px_cat_g1v2    : Product category reference
    - silver.erp_cust_az12      : Customer demographic data
    - silver.erp_loc_a101       : Customer location reference

TRANSFORMATION RULES:
    1. Customer Info (crm_cust_info):
       - Remove duplicate cst_id, keep latest by cst_create_date
       - Trim whitespace from name fields
       - Standardize marital status codes (S->Single, M->Married)
       - Standardize gender codes (M->Male, F->Female)
    
    2. Product Info (crm_prd_info):
       - Extract category ID from product key (first 5 chars)
       - Extract clean product key (from position 7 onward)
       - Replace nulls in cost with 0
       - Map product line codes to descriptive names
       - Calculate end dates using LEAD window function
    
    3. Sales Details (crm_sales_details):
       - Convert numeric date fields (YYYYMMDD) to DATE type
       - Validate and recalculate sales amount (quantity * price)
       - Derive missing price from sales/quantity
       - Handle invalid dates (0 or wrong length)
    
    4. ERP Customer (erp_cust_az12):
       - Remove 'NAS' prefix from customer IDs
       - Validate birth dates (not in future)
       - Standardize gender values
    
    5. ERP Location (erp_loc_a101):
       - Remove hyphens from customer IDs
       - Standardize country codes (DE->Germany, US/USA->United States)

EXECUTION:
    EXEC silver.load_silver;

DEPENDENCIES:
    - All Bronze layer tables must be populated
    - Silver schema and tables must exist

ERROR HANDLING:
    - Uses TRY-CATCH block for error management
    - Prints detailed error information on failure
    - All operations are logged with timestamps

PERFORMANCE CONSIDERATIONS:
    - Uses TRUNCATE instead of DELETE for faster table clearing
    - Efficient use of window functions (ROW_NUMBER, LEAD)
    - Consider adding indexes on join keys after load
    - Large datasets may benefit from batch processing

MAINTENANCE NOTES:
    - This is a full refresh pattern (TRUNCATE/INSERT)
    - No incremental load logic implemented
    - Consider adding row counts and data quality checks
    - Add error logging to a dedicated audit table
VERSION: 1.0
================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;  -- Automatically rollback on error
    
    -- Variable declarations
    DECLARE @start_time DATETIME2,
            @end_time DATETIME2,
            @batch_start_time DATETIME2,
            @batch_end_time DATETIME2,
            @rows_affected INT,
            @error_number INT,
            @error_message NVARCHAR(4000),
            @error_severity INT,
            @error_state INT;
    
    BEGIN TRY
        SET @batch_start_time = SYSDATETIME();
        
        PRINT '=============================================================='
        PRINT 'Silver Layer ETL Process Started'
        PRINT 'Start Time: ' + CONVERT(VARCHAR(23), @batch_start_time, 121)
        PRINT '=============================================================='
        PRINT ''
        
        -- ============================================================
        -- SECTION 1: Load CRM Tables
        -- ============================================================
        PRINT '--------------------------------------------------------------'
        PRINT 'SECTION 1: Loading CRM Tables'
        PRINT '--------------------------------------------------------------'
        
        -- Load silver.crm_cust_info
        SET @start_time = SYSDATETIME();
        PRINT CHAR(9) + '>> Processing: silver.crm_cust_info'
        PRINT CHAR(9) + '   Truncating table...'
        
        TRUNCATE TABLE silver.crm_cust_info;
        
        PRINT CHAR(9) + '   Inserting transformed data...'
        
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            LTRIM(RTRIM(cst_firstname)) AS cst_firstname,
            LTRIM(RTRIM(cst_lastname)) AS cst_lastname,
            CASE UPPER(LTRIM(RTRIM(cst_marital_status)))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'N/A'
            END AS cst_marital_status,
            CASE UPPER(LTRIM(RTRIM(cst_gndr)))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'N/A'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT 
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id 
                    ORDER BY cst_create_date DESC
                ) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) AS deduped
        WHERE rn = 1;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();
        
        PRINT CHAR(9) + '   Rows inserted: ' + CAST(@rows_affected AS VARCHAR(20))
        PRINT CHAR(9) + '   Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS VARCHAR(20)) + ' seconds'
        PRINT ''
        
        -- Load silver.crm_prd_info
        SET @start_time = SYSDATETIME();
        PRINT CHAR(9) + '>> Processing: silver.crm_prd_info'
        PRINT CHAR(9) + '   Truncating table...'
        
        TRUNCATE TABLE silver.crm_prd_info;
        
        PRINT CHAR(9) + '   Inserting transformed data...'
        
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(LTRIM(RTRIM(prd_line)))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATEADD(DAY, -1, 
                    LEAD(prd_start_dt) OVER (
                        PARTITION BY SUBSTRING(prd_key, 7, LEN(prd_key)) 
                        ORDER BY prd_start_dt
                    )
                ) AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();
        
        PRINT CHAR(9) + '   Rows inserted: ' + CAST(@rows_affected AS VARCHAR(20))
        PRINT CHAR(9) + '   Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS VARCHAR(20)) + ' seconds'
        PRINT ''
        
        -- Load silver.crm_sales_details
        SET @start_time = SYSDATETIME();
        PRINT CHAR(9) + '>> Processing: silver.crm_sales_details'
        PRINT CHAR(9) + '   Truncating table...'
        
        TRUNCATE TABLE silver.crm_sales_details;
        
        PRINT CHAR(9) + '   Inserting transformed data...'
        
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- Convert YYYYMMDD integer to DATE, handle invalid values
            CASE 
                WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR(8))) != 8 THEN NULL
                ELSE TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR(8))) != 8 THEN NULL
                ELSE TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR(8))) != 8 THEN NULL
                ELSE TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112)
            END AS sls_due_dt,
            -- Recalculate sales if original value is missing or incorrect
            CASE
                WHEN sls_sales IS NULL 
                     OR sls_sales <= 0 
                     OR ABS(sls_sales - (sls_quantity * ABS(sls_price))) > 0.01
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            -- Derive price if original value is invalid
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();
        
        PRINT CHAR(9) + '   Rows inserted: ' + CAST(@rows_affected AS VARCHAR(20))
        PRINT CHAR(9) + '   Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS VARCHAR(20)) + ' seconds'
        PRINT ''
        
        -- ============================================================
        -- SECTION 2: Load ERP Tables
        -- ============================================================
        PRINT '--------------------------------------------------------------'
        PRINT 'SECTION 2: Loading ERP Tables'
        PRINT '--------------------------------------------------------------'
        
        -- Load silver.erp_px_cat_g1v2
        SET @start_time = SYSDATETIME();
        PRINT CHAR(9) + '>> Processing: silver.erp_px_cat_g1v2'
        PRINT CHAR(9) + '   Truncating table...'
        
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        PRINT CHAR(9) + '   Inserting data...'
        
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();
        
        PRINT CHAR(9) + '   Rows inserted: ' + CAST(@rows_affected AS VARCHAR(20))
        PRINT CHAR(9) + '   Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS VARCHAR(20)) + ' seconds'
        PRINT ''
        
        -- Load silver.erp_cust_az12
        SET @start_time = SYSDATETIME();
        PRINT CHAR(9) + '>> Processing: silver.erp_cust_az12'
        PRINT CHAR(9) + '   Truncating table...'
        
        TRUNCATE TABLE silver.erp_cust_az12;
        
        PRINT CHAR(9) + '   Inserting transformed data...'
        
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            -- Remove 'NAS' prefix from customer IDs
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            -- Validate birth dates - reject future dates
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            -- Standardize gender values
            CASE 
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
            END AS gen
        FROM bronze.erp_cust_az12;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();
        
        PRINT CHAR(9) + '   Rows inserted: ' + CAST(@rows_affected AS VARCHAR(20))
        PRINT CHAR(9) + '   Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS VARCHAR(20)) + ' seconds'
        PRINT ''
        
        -- Load silver.erp_loc_a101
        SET @start_time = SYSDATETIME();
        PRINT CHAR(9) + '>> Processing: silver.erp_loc_a101'
        PRINT CHAR(9) + '   Truncating table...'
        
        TRUNCATE TABLE silver.erp_loc_a101;
        
        PRINT CHAR(9) + '   Inserting transformed data...'
        
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN LTRIM(RTRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(LTRIM(RTRIM(cntry))) IN ('US', 'USA') THEN 'United States'
                WHEN LTRIM(RTRIM(cntry)) = '' OR cntry IS NULL THEN 'N/A'
                ELSE LTRIM(RTRIM(cntry))
            END AS cntry
        FROM bronze.erp_loc_a101;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = SYSDATETIME();
        
        PRINT CHAR(9) + '   Rows inserted: ' + CAST(@rows_affected AS VARCHAR(20))
        PRINT CHAR(9) + '   Duration: ' + CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) / 1000.0 AS VARCHAR(20)) + ' seconds'
        PRINT ''
        
        -- ============================================================
        -- Process Complete
        -- ============================================================
        SET @batch_end_time = SYSDATETIME();
        
        PRINT '=============================================================='
        PRINT 'Silver Layer ETL Process Completed Successfully'
        PRINT 'End Time: ' + CONVERT(VARCHAR(23), @batch_end_time, 121)
        PRINT 'Total Duration: ' + CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) / 1000.0 AS VARCHAR(20)) + ' seconds'
        PRINT '=============================================================='
        
    END TRY
    BEGIN CATCH
        -- Capture error details
        SELECT 
            @error_number = ERROR_NUMBER(),
            @error_message = ERROR_MESSAGE(),
            @error_severity = ERROR_SEVERITY(),
            @error_state = ERROR_STATE();
        
        -- Print error information
        PRINT ''
        PRINT '=============================================================='
        PRINT 'ERROR OCCURRED DURING SILVER LAYER ETL PROCESS'
        PRINT '=============================================================='
        PRINT 'Error Number:    ' + CAST(@error_number AS VARCHAR(10))
        PRINT 'Error Severity:  ' + CAST(@error_severity AS VARCHAR(10))
        PRINT 'Error State:     ' + CAST(@error_state AS VARCHAR(10))
        PRINT 'Error Message:   ' + @error_message
        PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A')
        PRINT 'Error Line:      ' + CAST(ERROR_LINE() AS VARCHAR(10))
        PRINT '=============================================================='
        
        -- Re-throw error to calling process
        THROW;
        
    END CATCH
END;
GO
