# Databricks Transformation

## Notebook Purpose

The Databricks notebook reads raw OpenFDA JSON from ADLS, flattens the nested recall records, validates and deduplicates the dataset, and writes curated Parquet output.

## Notebook Inputs

- `storage_account_name`
- `input_path`
- `output_path`
- `invalid_path`
- `ingest_date`

## Secret Handling

The notebook retrieves the ADLS credential from a Databricks secret scope:

- scope: `openfda-scope`
- key: `adls-account-key`

The storage account name is passed in as a runtime parameter rather than hardcoded into notebook source.

## Transformation Logic

The notebook:

1. Reads the raw JSON file with Spark.
2. Explodes the `results` array into individual recall rows.
3. Normalizes blank-like values into nulls.
4. Flattens selected `openfda` arrays into pipe-delimited strings.
5. Parses FDA date fields into date columns.
6. Adds `ingest_date`, `report_year`, and `report_month`.

## Validation Rules

Records are invalid when:

- `event_id` is null
- `recall_number` is null
- `report_date` cannot be parsed

Duplicate rows are identified using:

- `event_id`
- `recall_number`

## Output

- Valid records are written to `curated/drug_enforcement/`
- Invalid records are written to `curated/drug_enforcement_invalid/`
- Curated output is partitioned by `report_year` and `report_month`

## Compatibility Note

The notebook uses `_metadata.file_path` instead of `input_file_name()` so it runs correctly on Unity Catalog-enabled compute.

