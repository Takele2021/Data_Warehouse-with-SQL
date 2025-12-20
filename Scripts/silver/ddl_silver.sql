/*==============================================================================
SCRIPT NAME: Create Silver Layer Tables
================================================================================
PURPOSE:
    This DDL script creates all tables in the Silver schema layer of the data
    warehouse. The Silver layer contains cleansed, standardized, and conformed
    data transformed from the Bronze (raw) layer.

SILVER LAYER OVERVIEW:
    The Silver layer serves as the curated data layer where:
    - Data quality rules are applied
    - Business logic transformations are implemented
    - Data is standardized and conformed
    - Duplicates are removed
    - Invalid data is handled
    - Reference data is normalized

TABLES CREATED:
    1. silver.crm_cust_info      - Customer master data from CRM system
    2. silver.crm_prd_info       - Product catalog information from CRM
    3. silver.crm_sales_details  - Sales transaction details from CRM
    4. silver.erp_px_cat_g1v2    - Product category reference from ERP
    5. silver.erp_cust_az12      - Customer demographic data from ERP
    6. silver.erp_loc_a101       - Customer location reference from ERP

DESIGN PRINCIPLES:
    - No primary keys defined (truncate/load pattern)
    - Audit columns included (dwh_create_date, dwh_update_date)
    - Proper data types based on content and usage
    - Nullable columns to handle data quality issues
    - Standard naming conventions for consistency

EXECUTION:
    Run this script to create or recreate all Silver layer tables.
    Existing tables will be dropped and recreated (data loss will occur).
    
    WARNING: This script will DROP existing tables. Ensure proper backups
             exist before execution in production environments.

DEPENDENCIES:
    - Silver schema must exist in the database
    - Appropriate permissions to CREATE and DROP tables

MAINTENANCE:
    - Add indexes after initial load for query performance
    - Consider partitioning for large fact tables
    - Update statistics regularly
    - Monitor table growth and archival needs

RELATED OBJECTS:
    - Procedure: silver.load_silver (ETL process)
    - Source Layer: Bronze schema tables
    - Target Layer: Gold schema tables

AUTHOR: [Your Name/Team]
CREATED: [Date]
MODIFIED: [Date] - [Description]
VERSION: 1.0
================================================================================
*/

-- Ensure we're using the correct database
USE [Warehouse];
GO

SET NOCOUNT ON;
GO

PRINT '=============================================================='
PRINT 'Silver Layer Table Creation Script'
PRINT 'Started: ' + CONVERT(VARCHAR(23), GETDATE(), 121)
PRINT '=============================================================='
PRINT ''

-- ============================================================
-- SECTION 1: CRM System Tables
-- ============================================================
PRINT '--------------------------------------------------------------'
PRINT 'SECTION 1: Creating CRM System Tables'
PRINT '--------------------------------------------------------------'
PRINT ''

-- -----------------------------------------------------------------------------
-- Table: silver.crm_cust_info
-- Purpose: Stores cleansed customer master data from CRM system
-- Source: bronze.crm_cust_info
-- Notes: Deduplicated by cst_id, standardized reference data
-- -----------------------------------------------------------------------------
PRINT 'Creating table: silver.crm_cust_info'

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
BEGIN
    PRINT '  >> Dropping existing table...'
    DROP TABLE silver.crm_cust_info;
END

CREATE TABLE silver.crm_cust_info
(
    -- Business Keys
    cst_id              INT             NOT NULL,           -- Customer ID (business key)
    cst_key             NVARCHAR(50)    NOT NULL,           -- Customer alternate key
    
    -- Customer Attributes
    cst_firstname       NVARCHAR(100)   NULL,               -- First name (trimmed)
    cst_lastname        NVARCHAR(100)   NULL,               -- Last name (trimmed)
    cst_marital_status  NVARCHAR(20)    NULL,               -- Standardized: Single, Married, N/A
    cst_gndr            NVARCHAR(20)    NULL,               -- Standardized: Male, Female, N/A
    cst_create_date     DATE            NULL,               -- Original record creation date
    
    -- Audit Columns
    dwh_create_date     DATETIME2(7)    NOT NULL DEFAULT SYSDATETIME(),  -- DWH insert timestamp
    dwh_update_date     DATETIME2(7)    NULL                              -- DWH update timestamp
);
GO

PRINT '  >> Table created successfully'
PRINT ''

-- -----------------------------------------------------------------------------
-- Table: silver.crm_prd_info
-- Purpose: Stores standardized product catalog information from CRM
-- Source: bronze.crm_prd_info
-- Notes: Category extracted, product line standardized, SCD Type 2 ready
-- -----------------------------------------------------------------------------
PRINT 'Creating table: silver.crm_prd_info'

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
BEGIN
    PRINT '  >> Dropping existing table...'
    DROP TABLE silver.crm_prd_info;
END

CREATE TABLE silver.crm_prd_info
(
    -- Business Keys
    prd_id              INT             NOT NULL,           -- Product ID (business key)
    cat_id              NVARCHAR(10)    NULL,               -- Category ID (extracted from prd_key)
    prd_key             NVARCHAR(50)    NOT NULL,           -- Product alternate key (cleaned)
    
    -- Product Attributes
    prd_nm              NVARCHAR(100)   NULL,               -- Product name
    prd_cost            DECIMAL(18,2)   NULL,               -- Product cost (DECIMAL for currency)
    prd_line            NVARCHAR(50)    NULL,               -- Standardized: Mountain, Road, Touring, etc.
    
    -- Temporal Attributes (SCD Type 2)
    prd_start_dt        DATE            NULL,               -- Product version effective date
    prd_end_dt          DATE            NULL,               -- Product version end date (NULL = current)
    
    -- Audit Columns
    dwh_create_date     DATETIME2(7)    NOT NULL DEFAULT SYSDATETIME(),
    dwh_update_date     DATETIME2(7)    NULL
);
GO

PRINT '  >> Table created successfully'
PRINT ''

-- -----------------------------------------------------------------------------
-- Table: silver.crm_sales_details
-- Purpose: Stores validated sales transaction details from CRM
-- Source: bronze.crm_sales_details
-- Notes: Dates converted, sales amounts validated and recalculated if needed
-- -----------------------------------------------------------------------------
PRINT 'Creating table: silver.crm_sales_details'

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
BEGIN
    PRINT '  >> Dropping existing table...'
    DROP TABLE silver.crm_sales_details;
END

CREATE TABLE silver.crm_sales_details
(
    -- Business Keys
    sls_ord_num         NVARCHAR(50)    NOT NULL,           -- Sales order number
    sls_prd_key         NVARCHAR(50)    NOT NULL,           -- Product key (FK to crm_prd_info)
    sls_cust_id         INT             NOT NULL,           -- Customer ID (FK to crm_cust_info)
    
    -- Date Attributes
    sls_order_dt        DATE            NULL,               -- Order date (converted from YYYYMMDD)
    sls_ship_dt         DATE            NULL,               -- Ship date (converted from YYYYMMDD)
    sls_due_dt          DATE            NULL,               -- Due date (converted from YYYYMMDD)
    
    -- Transaction Measures
    sls_sales           DECIMAL(18,2)   NULL,               -- Sales amount (validated/recalculated)
    sls_quantity        INT             NULL,               -- Quantity sold
    sls_price           DECIMAL(18,2)   NULL,               -- Unit price (derived if missing)
    
    -- Audit Columns
    dwh_create_date     DATETIME2(7)    NOT NULL DEFAULT SYSDATETIME(),
    dwh_update_date     DATETIME2(7)    NULL
);
GO

PRINT '  >> Table created successfully'
PRINT ''

-- ============================================================
-- SECTION 2: ERP System Tables
-- ============================================================
PRINT '--------------------------------------------------------------'
PRINT 'SECTION 2: Creating ERP System Tables'
PRINT '--------------------------------------------------------------'
PRINT ''

-- -----------------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-- Purpose: Product category reference data from ERP system
-- Source: bronze.erp_px_cat_g1v2
-- Notes: Minimal transformation, used for product categorization
-- -----------------------------------------------------------------------------
PRINT 'Creating table: silver.erp_px_cat_g1v2'

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
BEGIN
    PRINT '  >> Dropping existing table...'
    DROP TABLE silver.erp_px_cat_g1v2;
END

CREATE TABLE silver.erp_px_cat_g1v2
(
    -- Business Keys & Attributes
    id              NVARCHAR(50)    NOT NULL,               -- Category ID (business key)
    cat             NVARCHAR(100)   NULL,                   -- Category name
    subcat          NVARCHAR(100)   NULL,                   -- Subcategory name
    maintenance     NVARCHAR(50)    NULL,                   -- Maintenance indicator/code
    
    -- Audit Columns
    dwh_create_date DATETIME2(7)    NOT NULL DEFAULT SYSDATETIME(),
    dwh_update_date DATETIME2(7)    NULL
);
GO

PRINT '  >> Table created successfully'
PRINT ''

-- -----------------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-- Purpose: Customer demographic data from ERP system
-- Source: bronze.erp_cust_az12
-- Notes: Customer ID cleaned (NAS prefix removed), gender standardized
-- -----------------------------------------------------------------------------
PRINT 'Creating table: silver.erp_cust_az12'

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
BEGIN
    PRINT '  >> Dropping existing table...'
    DROP TABLE silver.erp_cust_az12;
END

CREATE TABLE silver.erp_cust_az12
(
    -- Business Keys
    cid             NVARCHAR(50)    NOT NULL,               -- Customer ID (cleaned, no prefix)
    
    -- Customer Attributes
    bdate           DATE            NULL,                   -- Birth date (validated, not future)
    gen             NVARCHAR(20)    NULL,                   -- Gender: Male, Female, N/A
    
    -- Audit Columns
    dwh_create_date DATETIME2(7)    NOT NULL DEFAULT SYSDATETIME(),
    dwh_update_date DATETIME2(7)    NULL
);
GO

PRINT '  >> Table created successfully'
PRINT ''

-- -----------------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-- Purpose: Customer location reference data from ERP system
-- Source: bronze.erp_loc_a101
-- Notes: Customer ID cleaned (hyphens removed), country codes standardized
-- -----------------------------------------------------------------------------
PRINT 'Creating table: silver.erp_loc_a101'

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
BEGIN
    PRINT '  >> Dropping existing table...'
    DROP TABLE silver.erp_loc_a101;
END

CREATE TABLE silver.erp_loc_a101
(
    -- Business Keys & Attributes
    cid             NVARCHAR(50)    NOT NULL,               -- Customer ID (cleaned, no hyphens)
    cntry           NVARCHAR(100)   NULL,                   -- Country (standardized names)
    
    -- Audit Columns
    dwh_create_date DATETIME2(7)    NOT NULL DEFAULT SYSDATETIME(),
    dwh_update_date DATETIME2(7)    NULL
);
GO

PRINT '  >> Table created successfully'
PRINT ''

-- ============================================================
-- Post-Creation Steps
-- ============================================================
PRINT '--------------------------------------------------------------'
PRINT 'Post-Creation Steps'
PRINT '--------------------------------------------------------------'
PRINT ''

PRINT 'Recommended next steps:'
PRINT '  1. Create indexes on frequently queried columns'
PRINT '  2. Create foreign key constraints if referential integrity needed'
PRINT '  3. Update table statistics after initial data load'
PRINT '  4. Consider columnstore indexes for large fact tables'
PRINT '  5. Implement data retention/archival policies'
PRINT ''

-- Example Index Creation (Commented Out - Enable as needed)
/*
-- Indexes for silver.crm_cust_info
CREATE NONCLUSTERED INDEX IX_crm_cust_info_cst_id 
    ON silver.crm_cust_info(cst_id);

CREATE NONCLUSTERED INDEX IX_crm_cust_info_cst_key 
    ON silver.crm_cust_info(cst_key);

-- Indexes for silver.crm_prd_info
CREATE NONCLUSTERED INDEX IX_crm_prd_info_prd_id 
    ON silver.crm_prd_info(prd_id);

CREATE NONCLUSTERED INDEX IX_crm_prd_info_prd_key 
    ON silver.crm_prd_info(prd_key);

CREATE NONCLUSTERED INDEX IX_crm_prd_info_cat_id 
    ON silver.crm_prd_info(cat_id);

-- Indexes for silver.crm_sales_details
CREATE NONCLUSTERED INDEX IX_crm_sales_details_ord_num 
    ON silver.crm_sales_details(sls_ord_num);

CREATE NONCLUSTERED INDEX IX_crm_sales_details_cust_id 
    ON silver.crm_sales_details(sls_cust_id);

CREATE NONCLUSTERED INDEX IX_crm_sales_details_prd_key 
    ON silver.crm_sales_details(sls_prd_key);

CREATE NONCLUSTERED INDEX IX_crm_sales_details_order_dt 
    ON silver.crm_sales_details(sls_order_dt);

-- Indexes for ERP tables
CREATE NONCLUSTERED INDEX IX_erp_px_cat_g1v2_id 
    ON silver.erp_px_cat_g1v2(id);

CREATE NONCLUSTERED INDEX IX_erp_cust_az12_cid 
    ON silver.erp_cust_az12(cid);

CREATE NONCLUSTERED INDEX IX_erp_loc_a101_cid 
    ON silver.erp_loc_a101(cid);
*/

PRINT '=============================================================='
PRINT 'Silver Layer Table Creation Completed Successfully'
PRINT 'Completed: ' + CONVERT(VARCHAR(23), GETDATE(), 121)
PRINT '=============================================================='
PRINT ''
PRINT 'All Silver layer tables have been created.'
PRINT 'Execute silver.load_silver procedure to populate tables.'
GO
