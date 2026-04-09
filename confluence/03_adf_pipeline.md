# ADF Pipeline

## Objective

The ADF pipeline ingests OpenFDA data into the raw lake layer and orchestrates the downstream Databricks transformation.

## Main Pipeline

- Pipeline name: `pl_openfda_drug_enforcement_etl`

## Activity Flow

1. `Copy_OpenFDA_To_ADLS_Raw`
2. `Get_Raw_File_Metadata`
3. `If_Raw_File_Is_Valid`
4. `Run_Databricks_OpenFDA_Transform`

## Linked Services

- HTTP linked service for `https://api.fda.gov`
- ADLS Gen2 linked service using ADF managed identity
- Azure Databricks linked service pointing to the live notebook compute

## Datasets

- HTTP binary source dataset for the API payload
- ADLS binary sink dataset for raw landing

## Pipeline Parameters

- `p_limit`

This parameter controls the record limit passed to the OpenFDA API.

## Validation Check

The pipeline validates the raw landing step by checking:

- `exists = true`
- `size > 0`

Only then does it proceed to the Databricks notebook activity.

## Notebook Parameters Passed From ADF

- `storage_account_name`
- `input_path`
- `output_path`
- `invalid_path`
- `ingest_date`

## Trigger Strategy

- Manual trigger for build validation and demos
- Optional schedule trigger after the manual flow is validated

## Git Integration

ADF is connected to GitHub using:

- collaboration branch: `main`
- publish branch: `adf_publish`
- root folder: `factory`

This stores the actual ADF JSON artifacts in the repository.

