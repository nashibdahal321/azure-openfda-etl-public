# Databricks notebook source
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.window import Window


dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("input_path", "")
dbutils.widgets.text("output_path", "")
dbutils.widgets.text("invalid_path", "")
dbutils.widgets.text("ingest_date", "")

storage_account_name = dbutils.widgets.get("storage_account_name").strip()
if not storage_account_name:
    raise ValueError("Missing required argument: storage_account_name")

storage_account_key = dbutils.secrets.get(scope="openfda-scope", key="adls-account-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)


def null_if_blank(column: Column) -> Column:
    cleaned = F.trim(column)
    return F.when(cleaned.isin("", "N/A", "null", "NULL"), F.lit(None)).otherwise(cleaned)


def array_to_pipe_string(column: Column) -> Column:
    return F.when(column.isNull() | (F.size(column) == 0), F.lit(None)).otherwise(F.array_join(column, "|"))


def build_curated_df(raw_df: DataFrame, ingest_date: str) -> DataFrame:
    exploded_df = raw_df.select(
        F.col("_metadata.file_path").alias("source_file"),
        F.explode_outer("results").alias("recall"),
    ).filter(F.col("recall").isNotNull())

    curated_df = exploded_df.select(
        null_if_blank(F.col("recall.event_id")).alias("event_id"),
        null_if_blank(F.col("recall.recall_number")).alias("recall_number"),
        null_if_blank(F.col("recall.status")).alias("status"),
        null_if_blank(F.col("recall.classification")).alias("classification"),
        null_if_blank(F.col("recall.product_type")).alias("product_type"),
        null_if_blank(F.col("recall.recalling_firm")).alias("recalling_firm"),
        null_if_blank(F.col("recall.city")).alias("city"),
        null_if_blank(F.col("recall.state")).alias("state"),
        null_if_blank(F.col("recall.country")).alias("country"),
        null_if_blank(F.col("recall.distribution_pattern")).alias("distribution_pattern"),
        null_if_blank(F.col("recall.reason_for_recall")).alias("reason_for_recall"),
        null_if_blank(F.col("recall.product_description")).alias("product_description"),
        F.to_date(null_if_blank(F.col("recall.report_date")), "yyyyMMdd").alias("report_date"),
        F.to_date(null_if_blank(F.col("recall.recall_initiation_date")), "yyyyMMdd").alias("recall_initiation_date"),
        F.to_date(null_if_blank(F.col("recall.center_classification_date")), "yyyyMMdd").alias(
            "center_classification_date"
        ),
        F.to_date(null_if_blank(F.col("recall.termination_date")), "yyyyMMdd").alias("termination_date"),
        array_to_pipe_string(F.col("recall.openfda.brand_name")).alias("brand_name"),
        array_to_pipe_string(F.col("recall.openfda.generic_name")).alias("generic_name"),
        array_to_pipe_string(F.col("recall.openfda.manufacturer_name")).alias("manufacturer_name"),
        array_to_pipe_string(F.col("recall.openfda.product_ndc")).alias("product_ndc"),
        array_to_pipe_string(F.col("recall.openfda.route")).alias("route"),
        null_if_blank(F.col("source_file")).alias("source_file"),
    )

    return (
        curated_df.withColumn("ingest_date", F.to_date(F.lit(ingest_date), "yyyy-MM-dd"))
        .withColumn("report_year", F.year("report_date"))
        .withColumn("report_month", F.month("report_date"))
    )


def split_valid_invalid(curated_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    validated_df = curated_df.withColumn(
        "validation_error",
        F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
        .when(F.col("recall_number").isNull(), F.lit("missing_recall_number"))
        .when(F.col("report_date").isNull(), F.lit("invalid_report_date")),
    )

    invalid_base_df = validated_df.filter(F.col("validation_error").isNotNull())
    valid_candidate_df = validated_df.filter(F.col("validation_error").isNull()).drop("validation_error")

    dedupe_window = Window.partitionBy("event_id", "recall_number").orderBy(
        F.col("report_date").desc_nulls_last(),
        F.col("center_classification_date").desc_nulls_last(),
        F.col("source_file").desc(),
    )

    ranked_df = valid_candidate_df.withColumn("record_rank", F.row_number().over(dedupe_window))

    duplicate_df = (
        ranked_df.filter(F.col("record_rank") > 1)
        .drop("record_rank")
        .withColumn("validation_error", F.lit("duplicate_event_recall"))
    )

    valid_df = ranked_df.filter(F.col("record_rank") == 1).drop("record_rank")
    invalid_df = invalid_base_df.unionByName(duplicate_df, allowMissingColumns=True)

    return valid_df, invalid_df


def write_output(df: DataFrame, output_path: str, partition_columns=None) -> None:
    writer = df.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(output_path)


def run_transformation(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    invalid_path: str,
    ingest_date: str,
) -> Dict[str, int]:
    required_args = {
        "input_path": input_path,
        "output_path": output_path,
        "invalid_path": invalid_path,
        "ingest_date": ingest_date,
    }

    missing_args = [name for name, value in required_args.items() if not value]
    if missing_args:
        raise ValueError(f"Missing required arguments: {', '.join(missing_args)}")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    raw_df = spark.read.option("multiLine", True).json(input_path)
    curated_df = build_curated_df(raw_df, ingest_date)

    raw_record_count = curated_df.count()
    if raw_record_count == 0:
        raise ValueError(f"No records found in input file: {input_path}")

    valid_df, invalid_df = split_valid_invalid(curated_df)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    write_output(valid_df, output_path, partition_columns=["report_year", "report_month"])
    write_output(invalid_df, invalid_path)

    summary = {
        "raw_record_count": raw_record_count,
        "valid_record_count": valid_count,
        "invalid_record_count": invalid_count,
    }

    print("OpenFDA transformation summary:", summary)
    return summary


summary = run_transformation(
    spark=spark,
    input_path=dbutils.widgets.get("input_path"),
    output_path=dbutils.widgets.get("output_path"),
    invalid_path=dbutils.widgets.get("invalid_path"),
    ingest_date=dbutils.widgets.get("ingest_date"),
)

display(spark.createDataFrame([summary]))
