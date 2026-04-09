# Runbook and Troubleshooting

## Standard Run Sequence

1. Run the ADF pipeline with `Trigger Now`
2. Confirm the raw JSON landed in ADLS
3. Confirm the Databricks notebook completed successfully
4. Confirm curated Parquet exists in ADLS
5. Run Synapse analytics queries

## Post-Run Validation

- Raw file exists under the current `ingest_date` folder
- Databricks summary returns valid and invalid record counts
- Curated output exists under `report_year=.../report_month=...`
- Synapse views return rows

## Common ADF Issues

- HTTP source path mismatch
- ADLS permissions for the ADF managed identity
- Dataset parameter mismatch between Copy and Get Metadata
- Notebook path mismatch in the Databricks activity

## Common Databricks Issues

- Secret scope or Key Vault secret not found
- Incorrect storage account parameter
- Unity Catalog blocking `input_file_name()`
- Wrong input path or empty raw file

## Common Synapse Issues

- Synapse managed identity lacks `Storage Blob Data Reader`
- Wrong storage account in the external data source
- Curated path pattern does not match the actual Parquet layout
- Incorrect `filepath()` syntax in the curated view

## Cost Control

- Terminate the Databricks cluster when not in use
- Stop any scheduled ADF triggers if they were created
- Avoid running Synapse queries when not needed

