/*
================================================================================
Project:        Data Warehouse - Gold Layer Views
Purpose:        Create production-ready dimension and fact tables for analytics

Description:
    This script creates the gold layer (analytics-ready) views for the data warehouse.
    These views serve as the source of truth for business intelligence and reporting.
    
    Views Created:
    - gold.dim_customers   : Customer dimension with CRM and ERP data integration
    - gold.dim_products    : Product dimension with category and lifecycle management
    - gold.fact_sales      : Sales transaction facts with dimensional keys

Prerequisites:
    - Silver layer tables must exist and be populated
    - gold schema must exist
    - User must have CREATE VIEW permissions
    
Change Log:
    v1.0.0 - Initial creation with three core views
    
Assumptions:
    - CRM is the master source for customer and gender data
    - Only active products (prd_end_dt IS NULL) are included in reports
    - Row numbers are used as surrogate keys for dimension tables
    
================================================================================
*/

USE [YourDatabaseName];
GO

/*
================================================================================
PRE-EXECUTION CHECKS
================================================================================
*/

-- Check if gold schema exists, create if not
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
    PRINT 'Schema [gold] created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema [gold] already exists.';
END
GO

/*
================================================================================
VIEW: gold.dim_customers
================================================================================
Purpose:
    Dimension table for customers combining CRM and ERP data
    
Business Rules:
    - CRM is the master source for customer and gender information
    - ERP provides supplementary customer details (birthdate, address)
    - Row number creates stable surrogate key (customer_key)
    
Column Definitions:
    customer_key      : Surrogate key for joining to fact tables
    customer_id       : Unique business identifier from CRM
    customer_number   : Customer reference number from CRM
    first_name        : Customer first name
    last_name         : Customer last name
    country           : Country of residence
    marital_status    : Customer marital status
    gender            : Gender (CRM is master, ERP as fallback)
    birthdate         : Date of birth from ERP
    create_date       : Customer creation date in CRM
    
Join Strategy:
    - CRM customer info is the base (main table)
    - ERP customer details joined on customer key
    - ERP location data joined on customer key
    
Performance Considerations:
    - Clustered index recommended on customer_key
    - Consider indexing customer_id for fact table joins
    
SCD Strategy: Type 1 (no historical tracking)
================================================================================
*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.dim_customers;
    PRINT 'View [gold.dim_customers] dropped successfully.';
END
GO

CREATE VIEW gold.dim_customers
AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    -- CRM is the master source for gender; use ERP only as fallback
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

/*
================================================================================
VIEW: gold.dim_products
================================================================================
Purpose:
    Dimension table for products with category and lifecycle management
    
Business Rules:
    - Only active products are included (where prd_end_dt IS NULL)
    - Products are ordered by start date and key to ensure consistent ordering
    - Row number creates stable surrogate key
    
Column Definitions:
    product_key      : Surrogate key for joining to fact tables
    product_id       : Unique business identifier
    product_number   : Product reference number
    product_name     : Human-readable product name
    category_id      : Category identifier
    category         : Product category from ERP master
    subcategory      : Product subcategory from ERP master
    cost             : Unit cost of the product
    product_line     : Product line classification
    start_date       : Product introduction date
    maintenance      : Maintenance flag/status from ERP
    
Join Strategy:
    - CRM product info is the base
    - ERP category data joined for master category information
    - WHERE clause filters only active products
    
Performance Considerations:
    - Clustered index recommended on product_key
    - Consider filtering logic when querying (prd_end_dt IS NULL is in view)
    - Index on product_number for fact table joins
    
SCD Strategy: Type 1 (no historical tracking)
================================================================================
*/
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.dim_products;
    PRINT 'View [gold.dim_products] dropped successfully.';
END
GO

CREATE VIEW gold.dim_products
AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date,
    pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;  -- Include only active products
GO

/*
================================================================================
VIEW: gold.fact_sales
================================================================================
Purpose:
    Fact table containing transactional sales data with dimensional references
    
Business Rules:
    - Each row represents a line item in a sales order
    - Dimensional keys (customer_key, product_key) are used for joining
    - All sales transactions from CRM are included
    
Column Definitions:
    order_number     : Unique order identifier (business key)
    product_key      : Surrogate key to dim_products
    customer_key     : Surrogate key to dim_customers
    order_date       : Date order was placed
    shipping_date    : Actual or expected shipping date
    due_date         : Order due date (SLA or customer requested)
    sales_amount     : Total revenue for this line item
    sales_quantity   : Number of units sold
    price            : Unit price
    
Join Strategy:
    - Sales details from CRM is the base table
    - Dimension lookups via product_number and customer_id
    - LEFT JOINs ensure all sales records are included even if dimensions are missing
    
Performance Considerations:
    - Clustered index recommended on order_number and order_date
    - Non-clustered indexes on product_key, customer_key for analytical queries
    - Consider partitioning by order_date for large fact tables
    - May want to add date keys for star schema optimization
    
Grain: One row per line item per order
================================================================================
*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.fact_sales;
    PRINT 'View [gold.fact_sales] dropped successfully.';
END
GO

CREATE VIEW gold.fact_sales
AS
SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS sales_quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

/*
================================================================================
VALIDATION AND DOCUMENTATION
================================================================================
*/

-- Verify views were created successfully
DECLARE @CreatedCount INT = 0;

SELECT @CreatedCount = COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold'
  AND TABLE_NAME IN ('dim_customers', 'dim_products', 'fact_sales')
  AND TABLE_TYPE = 'VIEW';

IF @CreatedCount = 3
BEGIN
    PRINT 'SUCCESS: All 3 gold layer views created successfully.';
END
ELSE
BEGIN
    PRINT 'WARNING: Expected 3 views, found ' + CAST(@CreatedCount AS NVARCHAR(10)) + '.';
END
GO

-- Display created views details
PRINT '';
PRINT '========================================';
PRINT 'GOLD LAYER VIEWS CREATED:';
PRINT '========================================';
SELECT 
    TABLE_SCHEMA AS SchemaName,
    TABLE_NAME AS ViewName,
    TABLE_TYPE AS ObjectType
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold'
  AND TABLE_NAME IN ('dim_customers', 'dim_products', 'fact_sales')
ORDER BY TABLE_NAME;

-- Optional: Display view definitions (commented out to keep output clean)
-- PRINT '';
-- PRINT 'View: dim_customers';
-- EXEC sp_helptext 'gold.dim_customers';
-- GO
-- 
-- PRINT '';
-- PRINT 'View: dim_products';
-- EXEC sp_helptext 'gold.dim_products';
-- GO
--
-- PRINT '';
-- PRINT 'View: fact_sales';
-- EXEC sp_helptext 'gold.fact_sales';
-- GO

/*
================================================================================
ROLLBACK SCRIPT (if needed)
================================================================================

DROP VIEW IF EXISTS gold.fact_sales;
DROP VIEW IF EXISTS gold.dim_products;
DROP VIEW IF EXISTS gold.dim_customers;

================================================================================
*/
