# Azure OpenFDA ETL

## Overview

This project implements an Azure-based ETL pipeline for OpenFDA drug enforcement data. The solution ingests recall data from the OpenFDA API, lands the raw payload in ADLS Gen2, transforms the data with Databricks PySpark, and exposes curated Parquet files through Synapse serverless SQL.

## Source System

- Source API: `https://api.fda.gov/drug/enforcement.json`
- Data domain: drug enforcement recall records
- Response format: JSON with a top-level `meta` object and `results` array

## High-Level Architecture

`Azure Data Factory -> ADLS Gen2 -> Databricks PySpark -> curated Parquet -> Synapse serverless SQL`

## Storage Layout

- `raw/drug_enforcement/ingest_date=YYYY-MM-DD/`
- `curated/drug_enforcement/`
- `curated/drug_enforcement_invalid/`

## Core Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Synapse serverless SQL
- GitHub
- GitHub Actions
- Confluence

## End-To-End Flow

1. ADF calls the OpenFDA API and lands the raw payload in ADLS.
2. ADF validates that the raw file exists and has data.
3. ADF triggers a Databricks notebook.
4. The Databricks notebook flattens, validates, deduplicates, and writes curated Parquet output.
5. Synapse serverless SQL reads the curated Parquet files through external objects and views.

## Repository References

- `databricks/nb_openfda_transform.py`
- `synapse/external_objects.sql`
- `synapse/analytics_queries.sql`
- `factory/` for ADF Git-integrated artifacts

