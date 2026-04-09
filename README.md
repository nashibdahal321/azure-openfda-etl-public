# Azure OpenFDA ETL

This repository contains an Azure-based ETL pipeline for OpenFDA drug enforcement data.

## Overview

The solution ingests recall data from the OpenFDA API with Azure Data Factory, lands the raw payload in Azure Data Lake Storage Gen2, transforms the dataset with Databricks PySpark, and exposes curated Parquet data through Synapse serverless SQL.

Source API:

- `https://api.fda.gov/drug/enforcement.json`

Core flow:

- `Azure Data Factory -> ADLS Gen2 -> Databricks PySpark -> curated Parquet -> Synapse serverless SQL`

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
  confluence/
    page_outline.md
```

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

## Synapse SQL

The Synapse layer is defined by:

- [external_objects.sql](/Users/Nawaraj/projects/ETL_Pipeline/azure-openfda-etl-public/synapse/external_objects.sql)
- [analytics_queries.sql](/Users/Nawaraj/projects/ETL_Pipeline/azure-openfda-etl-public/synapse/analytics_queries.sql)

`external_objects.sql` creates the serverless SQL database objects and views over curated Parquet files in ADLS.

`analytics_queries.sql` provides example analytical queries against the curated and invalid views.

## ADF Artifacts

The `factory/` folder is reserved for Azure Data Factory Git integration.

To connect the live ADF artifacts to this repository:

1. Open ADF Studio.
2. Go to `Manage -> Source control`.
3. Connect the factory to this GitHub repository.
4. Use:
   - collaboration branch: `main`
   - publish branch: `adf_publish`
   - root folder: `factory`
5. Select `Import existing resources to repository`.

After that, ADF will populate `factory/` with the actual JSON artifacts for pipelines, datasets, linked services, and triggers.

The ADF notebook activity should pass these base parameters to the Databricks notebook:

- `storage_account_name`
- `input_path`
- `output_path`
- `invalid_path`
- `ingest_date`

This keeps the notebook code versioned in GitHub while allowing environment-specific values to stay outside the notebook source.

## Notebook Versioning Workflow

The recommended pattern is:

1. Keep the Databricks notebook under version control in GitHub as `databricks/nb_openfda_transform.py`
2. Import or sync that notebook source into the Databricks workspace
3. Point the ADF notebook activity to the live workspace notebook path
4. When notebook logic changes, update the workspace notebook and export the latest version back into GitHub before opening a pull request

The detailed sync flow is documented in:

- [notebook_versioning.md](/Users/Nawaraj/projects/ETL_Pipeline/azure-openfda-etl-public/databricks/notebook_versioning.md)

## GitHub Workflow

This repo includes:

- a pull request template
- a lightweight GitHub Actions workflow that validates Python syntax and key SQL object definitions

## Confluence

The Confluence page structure for this project is outlined in:

- [page_outline.md](/Users/Nawaraj/projects/ETL_Pipeline/azure-openfda-etl-public/confluence/page_outline.md)
