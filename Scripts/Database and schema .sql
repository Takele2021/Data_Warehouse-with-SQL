/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

/*=========================================================
 STEP 1: Drop DataWarehouse database if it already exists
=========================================================*/
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'DataWarehouse')
BEGIN
    PRINT 'Database DataWarehouse exists — preparing to drop...';

    BEGIN TRY
        ALTER DATABASE DataWarehouse 
            SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

        DROP DATABASE DataWarehouse;

        PRINT 'Database DataWarehouse dropped successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred while dropping database.';
        THROW;
    END CATCH
END
GO

/*=========================================================
 STEP 2: Create DataWarehouse with explicit file configuration
=========================================================*/
PRINT 'Creating DataWarehouse database...';

CREATE DATABASE DataWarehouse
ON PRIMARY (
    NAME        = N'DW_Data',
    FILENAME    = 'C:\Program Files\Microsoft SQL Server\MSSQL17.MSSQLSERVER\MSSQL\DATA\DataWarehouse.mdf',   -- Adjust for your environment
    SIZE        = 1024MB,
    FILEGROWTH  = 256MB
)
LOG ON (
    NAME        = N'DW_Log',
    FILENAME    = 'C:\Program Files\Microsoft SQL Server\MSSQL17.MSSQLSERVER\MSSQL\DATA\DataWarehouse.ldf',   -- Adjust for your environment
    SIZE        = 512MB,
    FILEGROWTH  = 256MB
);
GO

PRINT 'Database DataWarehouse created successfully.';
GO

USE DataWarehouse;
GO

/*=========================================================
 STEP 3: Create required schemas (idempotent pattern)
=========================================================*/

-- Bronze Layer
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze AUTHORIZATION dbo;');
    PRINT 'Schema [bronze] created.';
END
ELSE
    PRINT 'Schema [bronze] already exists.';

-- Silver Layer
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'silver')
BEGIN
    EXEC('CREATE SCHEMA silver AUTHORIZATION dbo;');
    PRINT 'Schema [silver] created.';
END
ELSE
    PRINT 'Schema [silver] already exists.';

-- Gold Layer
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'gold')
BEGIN
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo;');
    PRINT 'Schema [gold] created.';
END
ELSE
    PRINT 'Schema [gold] already exists.';
GO

PRINT 'All schemas validated and ready.';
GO

/*=========================================================
 STEP 4: Optional – DW Recommended Settings (Production)
=========================================================*/

-- Enable snapshot isolation (OLAP-friendly)
ALTER DATABASE DataWarehouse SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE DataWarehouse SET READ_COMMITTED_SNAPSHOT ON;

-- Force page verification for integrity
ALTER DATABASE DataWarehouse SET PAGE_VERIFY CHECKSUM;

-- Recommended for Data Warehouse workloads
ALTER DATABASE DataWarehouse SET RECOVERY SIMPLE;
GO

PRINT 'DataWarehouse setup completed successfully.';
GO

