/*
Run this script in the built-in serverless SQL pool.

Before running:
1. Replace <storage-account> with your ADLS Gen2 storage account name.
2. Grant the Synapse workspace managed identity the role Storage Blob Data Reader on the storage account.
3. Make sure the curated Parquet files already exist in ADLS.
*/

IF DB_ID('openfda_serverless') IS NULL
BEGIN
    CREATE DATABASE openfda_serverless;
END
GO

USE openfda_serverless;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.symmetric_keys
    WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    CREATE MASTER KEY;
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_scoped_credentials
    WHERE name = 'cred_openfda_mi'
)
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL cred_openfda_mi
    WITH IDENTITY = 'Managed Identity';
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.external_data_sources
    WHERE name = 'eds_openfda_lake'
)
BEGIN
    CREATE EXTERNAL DATA SOURCE eds_openfda_lake
    WITH (
        LOCATION = 'https://<storage-account>.dfs.core.windows.net/openfda',
        CREDENTIAL = cred_openfda_mi
    );
END
GO

CREATE OR ALTER VIEW dbo.vw_openfda_recall_curated
AS
SELECT
    [event_id],
    [recall_number],
    [status],
    [classification],
    [product_type],
    [recalling_firm],
    [city],
    [state],
    [country],
    [distribution_pattern],
    [reason_for_recall],
    [product_description],
    [report_date],
    [recall_initiation_date],
    [center_classification_date],
    [termination_date],
    [brand_name],
    [generic_name],
    [manufacturer_name],
    [product_ndc],
    [route],
    [ingest_date],
    [source_file],
    TRY_CAST(REPLACE([rows].filepath(1), 'report_year=', '') AS INT) AS report_year,
    TRY_CAST(REPLACE([rows].filepath(2), 'report_month=', '') AS INT) AS report_month
FROM OPENROWSET(
    BULK 'curated/drug_enforcement/report_year=*/report_month=*/*.parquet',
    DATA_SOURCE = 'eds_openfda_lake',
    FORMAT = 'PARQUET'
) WITH (
    event_id VARCHAR(50),
    recall_number VARCHAR(50),
    status VARCHAR(50),
    classification VARCHAR(50),
    product_type VARCHAR(100),
    recalling_firm VARCHAR(300),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    distribution_pattern VARCHAR(500),
    reason_for_recall VARCHAR(2000),
    product_description VARCHAR(2000),
    report_date DATE,
    recall_initiation_date DATE,
    center_classification_date DATE,
    termination_date DATE,
    brand_name VARCHAR(1000),
    generic_name VARCHAR(1000),
    manufacturer_name VARCHAR(1000),
    product_ndc VARCHAR(1000),
    route VARCHAR(500),
    ingest_date DATE,
    source_file VARCHAR(1000)
) AS [rows];
GO

CREATE OR ALTER VIEW dbo.vw_openfda_recall_invalid
AS
SELECT
    [event_id],
    [recall_number],
    [status],
    [classification],
    [product_type],
    [recalling_firm],
    [city],
    [state],
    [country],
    [distribution_pattern],
    [reason_for_recall],
    [product_description],
    [report_date],
    [recall_initiation_date],
    [center_classification_date],
    [termination_date],
    [brand_name],
    [generic_name],
    [manufacturer_name],
    [product_ndc],
    [route],
    [ingest_date],
    [source_file],
    [validation_error]
FROM OPENROWSET(
    BULK 'curated/drug_enforcement_invalid/*.parquet',
    DATA_SOURCE = 'eds_openfda_lake',
    FORMAT = 'PARQUET'
) WITH (
    event_id VARCHAR(50),
    recall_number VARCHAR(50),
    status VARCHAR(50),
    classification VARCHAR(50),
    product_type VARCHAR(100),
    recalling_firm VARCHAR(300),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    distribution_pattern VARCHAR(500),
    reason_for_recall VARCHAR(2000),
    product_description VARCHAR(2000),
    report_date DATE,
    recall_initiation_date DATE,
    center_classification_date DATE,
    termination_date DATE,
    brand_name VARCHAR(1000),
    generic_name VARCHAR(1000),
    manufacturer_name VARCHAR(1000),
    product_ndc VARCHAR(1000),
    route VARCHAR(500),
    ingest_date DATE,
    source_file VARCHAR(1000),
    validation_error VARCHAR(100)
) AS [rows];
GO
