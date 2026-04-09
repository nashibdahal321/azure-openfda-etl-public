# Databricks Notebook Versioning Workflow

## Goal

Keep the Databricks notebook versioned in GitHub while still running the live notebook from the Databricks workspace through ADF.

## Recommended Model

Use GitHub as the source-controlled notebook definition and Databricks workspace as the execution target.

That means:

1. GitHub stores `databricks/nb_openfda_transform.py`
2. Databricks stores the live notebook at a workspace path such as `/Users/<email>/openfda-etl/nb_openfda_transform`
3. ADF points to the live Databricks notebook path
4. ADF artifacts are stored separately in `factory/` through ADF Git integration

## Initial Setup

1. Open Databricks Workspace.
2. Create or open the notebook you want ADF to run.
3. Import the contents of `databricks/nb_openfda_transform.py` into that notebook, or import the file directly as a notebook source file.
4. Confirm the notebook still runs successfully in Databricks.
5. In ADF, update the notebook activity to use the exact workspace notebook path.
6. In ADF, pass these base parameters:
   - `storage_account_name`
   - `input_path`
   - `output_path`
   - `invalid_path`
   - `ingest_date`

## Day-To-Day Change Flow

When you want to change notebook logic:

1. Create a feature branch in GitHub.
2. Update the notebook in Databricks.
3. Export the updated notebook source back into `databricks/nb_openfda_transform.py`.
4. Commit that exported file into your feature branch.
5. If the ADF notebook activity or parameters changed, commit the corresponding ADF JSON artifact changes from `factory/` too.
6. Open a pull request.
7. After merge, make sure the Databricks workspace notebook matches the merged GitHub version.

## Pull Request Guidance

A notebook-related PR should usually include:

- updated `databricks/nb_openfda_transform.py`
- any matching ADF artifact changes under `factory/`
- a short note on how the notebook was validated
- one screenshot of the Databricks run summary when useful

## Validation Checklist

Before opening a PR:

1. Confirm the notebook still runs manually in Databricks.
2. Confirm ADF still passes the required base parameters.
3. Confirm the pipeline still writes curated output to ADLS.
4. Confirm Synapse queries still return expected results if the schema changed.

## Why This Model Works

- GitHub shows the notebook history and pull requests
- Databricks remains the live execution environment
- ADF still orchestrates the notebook as an Azure artifact
- Environment-specific values stay out of notebook source and are passed through parameters or secrets
