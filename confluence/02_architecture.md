# Architecture

## Architecture Summary

The design separates orchestration, storage, transformation, and analytics into focused Azure services.

## Service Responsibilities

- Azure Data Factory handles ingestion and orchestration.
- ADLS Gen2 stores the raw and curated layers.
- Databricks handles PySpark transformation and data validation.
- Synapse serverless SQL provides a lightweight SQL query layer over curated Parquet.

## Data Flow

1. ADF calls the OpenFDA enforcement endpoint.
2. ADF writes the raw JSON payload into ADLS.
3. Databricks reads the raw JSON from ADLS.
4. Databricks explodes the `results` array into one row per recall record.
5. Databricks validates identifiers and dates, deduplicates records, and writes Parquet output.
6. Synapse reads the curated output directly from ADLS with `OPENROWSET`.

## ADLS Layout

```text
openfda/
  raw/
    drug_enforcement/
      ingest_date=YYYY-MM-DD/
        openfda_enforcement_<run-id>.json
  curated/
    drug_enforcement/
      report_year=YYYY/
        report_month=MM/
          *.parquet
    drug_enforcement_invalid/
      *.parquet
```

## Authentication Model

- ADF uses managed identity to write into ADLS.
- Synapse uses the workspace managed identity to read curated Parquet.
- Databricks uses a Key Vault-backed secret scope to retrieve the ADLS credential at runtime.

## Design Trade-Offs

- The solution uses one public API and one pipeline to keep the implementation focused.
- The curated layer uses Parquet for efficient downstream SQL analysis.
- The raw layer is preserved for traceability and reprocessing.
- Notebook code is versioned in GitHub while the live notebook executes in Databricks.

