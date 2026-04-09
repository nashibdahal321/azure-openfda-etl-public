# Azure OpenFDA ETL

This repository contains a focused Azure ETL implementation for OpenFDA drug enforcement data.

## Overview

The pipeline ingests recall data from the OpenFDA API with Azure Data Factory, lands the raw payload in Azure Data Lake Storage Gen2, transforms the dataset with Databricks PySpark, and exposes curated Parquet data through Synapse serverless SQL.

Source API:

- `https://api.fda.gov/drug/enforcement.json`

Core flow:

- `Azure Data Factory -> ADLS Gen2 -> Databricks PySpark -> curated Parquet -> Synapse serverless SQL`

## Current Implementation

- Source: OpenFDA drug enforcement API
- Orchestration: Azure Data Factory
- Storage: Azure Data Lake Storage Gen2
- Transformation: Azure Databricks with PySpark
- Query layer: Synapse serverless SQL

The implementation keeps the raw API payload in the lake, applies validation and transformation logic in Databricks, and exposes the curated dataset as Parquet for downstream SQL analysis.

## Execution Flow

1. ADF calls the OpenFDA API and lands the raw JSON payload in ADLS.
2. ADF validates that the raw file exists and is non-empty.
3. ADF triggers the Databricks notebook with runtime parameters.
4. The Databricks notebook flattens, validates, and deduplicates the recall records.
5. Valid records are written to curated Parquet and invalid records are written to a separate invalid path.
6. Synapse serverless SQL reads the curated Parquet directly from ADLS through external objects and views.

## Repository Contents

```text
azure-openfda-etl-public/
  README.md
  .github/
    workflows/ci.yml
    pull_request_template.md
  databricks/
    nb_openfda_transform.py
  synapse/
    external_objects.sql
    analytics_queries.sql
  factory/
    .gitkeep
```

This repository keeps the notebook source and Synapse SQL under version control. The `factory/` directory is reserved for the exported Azure Data Factory artifacts after source control is connected from ADF Studio.

## Databricks Notebook

The Databricks notebook is stored as a Python notebook source file:

- [nb_openfda_transform.py](/Users/Nawaraj/projects/ETL_Pipeline/azure-openfda-etl-public/databricks/nb_openfda_transform.py)

The same notebook can be:

- versioned in GitHub as notebook source
- imported into the Databricks workspace for execution
- referenced by Azure Data Factory through its live notebook path

It expects ADF to pass these parameters:

- `storage_account_name`
- `input_path`
- `output_path`
- `invalid_path`
- `ingest_date`

The notebook reads an ADLS credential from a Databricks secret scope and writes:

- curated Parquet to `curated/drug_enforcement/`
- invalid rows to `curated/drug_enforcement_invalid/`

The notebook uses `_metadata.file_path` for compatibility with Unity Catalog-enabled compute.

The Databricks setup assumes:

- secret scope: `openfda-scope`
- secret key: `adls-account-key`

## Synapse SQL

The Synapse layer is defined by:

- [external_objects.sql](/Users/Nawaraj/projects/ETL_Pipeline/azure-openfda-etl-public/synapse/external_objects.sql)
- [analytics_queries.sql](/Users/Nawaraj/projects/ETL_Pipeline/azure-openfda-etl-public/synapse/analytics_queries.sql)

`external_objects.sql` creates the serverless SQL database objects and views over curated Parquet files in ADLS.

`analytics_queries.sql` provides example analytical queries against the curated and invalid views.

Typical execution order:

1. Run `external_objects.sql` once to create the database objects and views.
2. Run `analytics_queries.sql` to query the curated output after pipeline execution.

## ADF Artifacts

The `factory/` folder is the location used for Azure Data Factory Git-integrated artifacts.

When ADF source control is connected to this repository, the `factory/` folder stores the JSON artifacts for:

- pipelines
- datasets
- linked services
- triggers

Recommended ADF Git settings:

- collaboration branch: `main`
- publish branch: `adf_publish`
- root folder: `factory`

Until the factory is exported from ADF Git integration, the folder may only contain `.gitkeep`.

The Databricks notebook activity passes these base parameters:

- `storage_account_name`
- `input_path`
- `output_path`
- `invalid_path`
- `ingest_date`

This keeps the notebook code versioned in GitHub while allowing environment-specific values to stay outside the notebook source.

## Development Workflow

This repo includes:

- a pull request template
- a lightweight GitHub Actions workflow that validates notebook Python syntax and key Synapse SQL object definitions

Recommended flow:

1. Make notebook, SQL, or ADF changes in a feature branch.
2. Push the branch and open a pull request into `main`.
3. Let GitHub Actions validate the repository changes.
4. Merge after review and publish ADF changes from the collaboration branch when needed.
