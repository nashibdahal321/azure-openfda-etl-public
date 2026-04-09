# Synapse Analytics

## Purpose

Synapse serverless SQL provides a lightweight query layer over curated Parquet files stored in ADLS.

## Main SQL Assets

- `synapse/external_objects.sql`
- `synapse/analytics_queries.sql`

## External Objects

The Synapse setup creates:

- database: `openfda_serverless`
- database scoped credential using managed identity
- external data source pointing to the ADLS container
- curated view over `curated/drug_enforcement/`
- invalid view over `curated/drug_enforcement_invalid/`

## Curated View

The curated view reads partitioned Parquet from:

- `curated/drug_enforcement/report_year=*/report_month=*/*.parquet`

It also derives:

- `report_year`
- `report_month`

from the folder path.

## Example Analytics

The analytics script includes:

- total recalls by classification
- top recalling firms
- recall volume by report year and month
- recalls by state
- most recent recalls
- invalid record counts by validation error

## Why Serverless SQL

- No dedicated SQL pool is required
- Curated Parquet can be queried directly in the lake
- The setup stays lightweight and easy to explain

