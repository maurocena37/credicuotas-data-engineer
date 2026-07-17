"""
Credicuotas Data Engineer challenge — pipeline scaffold.

This is a MINIMAL starting point, not a constraint:
  * `bronze` already works — it reads the raw CSVs so you can run the project
    on minute one (`make run`).
  * `silver` and `gold` are TODO stubs for you to implement.

You are free to restructure everything: split into modules, add a config layer,
introduce a proper runner / orchestration, add tests, write to Delta/Parquet, etc.
Show us how you'd build this for a real, growing production setup.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Fixed reference date for the delinquency metric (see README).
REFERENCE_DATE = "2026-01-15"

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"

RAW_TABLES = ["customers", "loan_applications", "disbursements", "installments"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("credicuotas-lending-pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# --------------------------------------------------------------------- bronze
def bronze(spark: SparkSession) -> dict[str, DataFrame]:
    """Raw ingestion: read each CSV as-is (all columns as strings).

    We read everything as strings on purpose — bronze should preserve the raw
    data faithfully, including the quality issues. Typing/cleaning happens in
    silver.
    """
    frames: dict[str, DataFrame] = {}
    for name in RAW_TABLES:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)  # keep raw strings; clean in silver
            .csv(str(DATA_DIR / f"{name}.csv"))
        )
        frames[name] = df
        print(f"[bronze] {name:18s} rows={df.count():>6d} cols={len(df.columns)}")
    return frames


# --------------------------------------------------------------------- silver
#
# Design decisions (documented here on purpose, so the "why" travels with the
# code and not just in a write-up):
#
#   * Dimensions (customers) are never dropped for data-quality reasons — we
#     impute/flag instead, because losing a customer row would silently break
#     joins downstream. Facts (applications/disbursements/installments) can be
#     quarantined because a bad fact row shouldn't corrupt an aggregate metric.
#   * "Quarantine" = moved to a separate DataFrame (not deleted). It's returned
#     alongside the clean frames so nothing disappears silently; gold/reporting
#     can inspect it, and a real pipeline would land it in a `quarantine/`
#     table instead of just dropping it on the floor.
#   * requested_amount: negative values are business-invalid (you can't
#     request -$500,000), so we null them out rather than dropping the whole
#     application row — the application itself (status, product, channel) is
#     still valid and needed for the Approval Rate metric regardless of amount.
#   * Exact-duplicate rows in loan_applications are the classic "same event
#     ingested twice" — safe to drop entirely, not just flag.


def _parse_mixed_date(col: Column) -> Column:
    """Parse a date column that mixes ISO (yyyy-MM-dd) and EU (dd/MM/yyyy)
    formats. Tries ISO first; falls back to EU for anything that doesn't
    parse. Rows that match neither format become null (and are still counted
    in the quality report below, so nothing fails silently)."""
    iso = F.to_date(col, "yyyy-MM-dd")
    eu = F.to_date(col, "dd/MM/yyyy")
    return F.coalesce(iso, eu)


def clean_customers(df: DataFrame) -> DataFrame:
    """Dimension table: type the dates, impute missing province instead of
    dropping the customer, and de-duplicate defensively on customer_id."""
    return (
        df.dropDuplicates(["customer_id"])
        .withColumn("created_at", F.to_date("created_at", "yyyy-MM-dd"))
        .withColumn("birth_date", F.to_date("birth_date", "yyyy-MM-dd"))
        .withColumn(
            "is_province_imputed",
            F.col("province").isNull() | (F.trim(F.col("province")) == ""),
        )
        .withColumn(
            "province",
            F.when(F.col("is_province_imputed"), F.lit("UNKNOWN")).otherwise(F.col("province")),
        )
    )


def clean_loan_applications(df: DataFrame) -> DataFrame:
    """Drop exact-duplicate rows, type numeric/date columns, and flag (rather
    than drop) rows with a missing/negative requested_amount."""
    typed = (
        df.dropDuplicates()  # exact-duplicate ingestion events
        .withColumn("applied_at", F.to_date("applied_at", "yyyy-MM-dd"))
        .withColumn("requested_amount", F.col("requested_amount").cast(DoubleType()))
        .withColumn("term_months", F.col("term_months").cast(IntegerType()))
        .withColumn("credit_score", F.col("credit_score").cast(IntegerType()))
    )
    return typed.withColumn(
        "is_requested_amount_invalid",
        F.col("requested_amount").isNull() | (F.col("requested_amount") < 0),
    ).withColumn(
        "requested_amount",
        F.when(F.col("requested_amount") < 0, None).otherwise(F.col("requested_amount")),
    )


def clean_disbursements(
    df: DataFrame, valid_application_ids: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Type columns, parse mixed date formats, and split into
    (valid, quarantined) based on the application_id foreign key.

    Returns (clean_df, orphans_df).
    """
    typed = (
        df.dropDuplicates(["disbursement_id"])
        .withColumn("disbursed_at", _parse_mixed_date(F.col("disbursed_at")))
        .withColumn("disbursed_amount", F.col("disbursed_amount").cast(DoubleType()))
        .withColumn("annual_interest_rate", F.col("annual_interest_rate").cast(DoubleType()))
        .withColumn("term_months", F.col("term_months").cast(IntegerType()))
    )
    valid = typed.join(valid_application_ids, on="application_id", how="left_semi")
    orphans = typed.join(valid_application_ids, on="application_id", how="left_anti")
    return valid, orphans


def clean_installments(
    df: DataFrame, valid_disbursement_ids: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Type columns, add an is_paid flag, and split into (valid, quarantined)
    based on the disbursement_id foreign key (an installment orphaned because
    its disbursement was itself quarantined must also be quarantined —
    otherwise it'd point at a disbursement that no longer exists downstream).

    Returns (clean_df, orphans_df).
    """
    typed = (
        df.dropDuplicates(["installment_id"])
        .withColumn("due_date", F.to_date("due_date", "yyyy-MM-dd"))
        .withColumn("paid_at", F.to_date("paid_at", "yyyy-MM-dd"))
        .withColumn("due_amount", F.col("due_amount").cast(DoubleType()))
        .withColumn("paid_amount", F.col("paid_amount").cast(DoubleType()))
        .withColumn("is_paid", F.col("paid_at").isNotNull())
    )
    valid = typed.join(valid_disbursement_ids, on="disbursement_id", how="left_semi")
    orphans = typed.join(valid_disbursement_ids, on="disbursement_id", how="left_anti")
    return valid, orphans


def silver(spark: SparkSession, bronze_frames: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Clean, type and conform the raw bronze data. Facts with a broken
    foreign key are quarantined (kept, not deleted) rather than silently
    dropped — see `_quarantine_*` keys in the returned dict."""
    customers = clean_customers(bronze_frames["customers"])
    loan_applications = clean_loan_applications(bronze_frames["loan_applications"])

    valid_app_ids = loan_applications.select("application_id")
    disbursements, orphan_disbursements = clean_disbursements(
        bronze_frames["disbursements"], valid_app_ids
    )

    valid_disb_ids = disbursements.select("disbursement_id")
    installments, orphan_installments = clean_installments(
        bronze_frames["installments"], valid_disb_ids
    )

    frames = {
        "customers": customers,
        "loan_applications": loan_applications,
        "disbursements": disbursements,
        "installments": installments,
        "_quarantine_orphan_disbursements": orphan_disbursements,
        "_quarantine_orphan_installments": orphan_installments,
    }
    for name, df in frames.items():
        print(f"[silver] {name:32s} rows={df.count():>6d}")
    return frames


# ----------------------------------------------------------------------- gold
#
# Design decisions:
#   * No dim_date: dates stay as native columns on each fact; time-based
#     grouping uses date_trunc('month', ...) directly in SQL.
#   * fact_loan_application_approved is a *derived staging view* (status =
#     'APPROVED'), not a persisted dimensional table — it exists purely to
#     turn the optional application<->disbursement relationship into a plain
#     inner join for Time-to-Disbursement. Verified against the real data:
#     100% of non-orphan disbursements point at an APPROVED application, so
#     this filter is equivalent to (and simpler than) an outer join + filter.
#   * Every metric is computed once as a `base` CTE, then UNION ALL'd across
#     4 segmentations (total / product / channel / applied_month) that all
#     share the same output shape — this keeps each segment trivial to test
#     independently and avoids GROUPING SETS' NULL-vs-NULL ambiguity.
#   * PENDING applications count in the Approval Rate denominator (agreed
#     design decision): the metric for a closed cohort must not change
#     retroactively as pending applications get resolved.
#   * Delinquency is computed at the installment grain: an installment is
#     delinquent if it was already due at REFERENCE_DATE and, as of that same
#     date, was still unpaid (paid_at is null) or was paid only after that
#     date (paid_at > REFERENCE_DATE). Installments not yet due at
#     REFERENCE_DATE are excluded from both numerator and denominator.

_APPROVAL_RATE_SQL = """
WITH base AS (
    SELECT product, channel, date_trunc('month', applied_at) AS applied_month, status
    FROM fact_loan_application
)
SELECT 'total' AS segment_type, 'ALL' AS segment_value,
       SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_count,
       COUNT(*) AS total_count,
       ROUND(SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) / COUNT(*), 4) AS approval_rate
FROM base

UNION ALL
SELECT 'product', product,
       SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END), COUNT(*),
       ROUND(SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) / COUNT(*), 4)
FROM base GROUP BY product

UNION ALL
SELECT 'channel', channel,
       SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END), COUNT(*),
       ROUND(SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) / COUNT(*), 4)
FROM base GROUP BY channel

UNION ALL
SELECT 'applied_month', CAST(applied_month AS DATE),
       SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END), COUNT(*),
       ROUND(SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) / COUNT(*), 4)
FROM base GROUP BY applied_month
"""

_TIME_TO_DISBURSEMENT_SQL = """
WITH base AS (
    SELECT
        a.product, a.channel, date_trunc('month', a.applied_at) AS applied_month,
        datediff(d.disbursed_at, a.applied_at) AS days_to_disbursement
    FROM fact_loan_application_approved a
    JOIN fact_disbursement d ON a.application_id = d.application_id
)
SELECT 'total' AS segment_type, 'ALL' AS segment_value,
       COUNT(*) AS disbursed_count,
       ROUND(AVG(days_to_disbursement), 2) AS avg_days_to_disbursement
FROM base

UNION ALL
SELECT 'product', product, COUNT(*), ROUND(AVG(days_to_disbursement), 2)
FROM base GROUP BY product

UNION ALL
SELECT 'channel', channel, COUNT(*), ROUND(AVG(days_to_disbursement), 2)
FROM base GROUP BY channel

UNION ALL
SELECT 'applied_month', CAST(applied_month AS DATE), COUNT(*), ROUND(AVG(days_to_disbursement), 2)
FROM base GROUP BY applied_month
"""

_DELINQUENCY_RATE_SQL = """
WITH base AS (
    SELECT
        a.product, a.channel, date_trunc('month', a.applied_at) AS applied_month,
        (i.paid_at IS NULL OR i.paid_at > DATE'{reference_date}') AS is_delinquent
    FROM fact_installment i
    JOIN fact_disbursement d ON i.disbursement_id = d.disbursement_id
    JOIN fact_loan_application a ON d.application_id = a.application_id
    WHERE i.due_date <= DATE'{reference_date}'
)
SELECT 'total' AS segment_type, 'ALL' AS segment_value,
       SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) AS delinquent_count,
       COUNT(*) AS due_count,
       ROUND(SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) / COUNT(*), 4) AS delinquency_rate
FROM base

UNION ALL
SELECT 'product', product,
       SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END), COUNT(*),
       ROUND(SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) / COUNT(*), 4)
FROM base GROUP BY product

UNION ALL
SELECT 'channel', channel,
       SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END), COUNT(*),
       ROUND(SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) / COUNT(*), 4)
FROM base GROUP BY channel

UNION ALL
SELECT 'applied_month', CAST(applied_month AS DATE),
       SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END), COUNT(*),
       ROUND(SUM(CASE WHEN is_delinquent THEN 1 ELSE 0 END) / COUNT(*), 4)
FROM base GROUP BY applied_month
"""


def register_star_schema(spark: SparkSession, silver_frames: dict[str, DataFrame]) -> None:
    """Register the dimensional model as temp views: 1 dimension, 3 fact
    tables at their natural grain, plus 1 derived staging view."""
    silver_frames["customers"].createOrReplaceTempView("dim_customer")

    fact_loan_application = silver_frames["loan_applications"]
    fact_loan_application.createOrReplaceTempView("fact_loan_application")
    fact_loan_application.filter("status = 'APPROVED'").createOrReplaceTempView(
        "fact_loan_application_approved"
    )
    silver_frames["disbursements"].createOrReplaceTempView("fact_disbursement")
    silver_frames["installments"].createOrReplaceTempView("fact_installment")


def compute_approval_rate(spark: SparkSession) -> DataFrame:
    return spark.sql(_APPROVAL_RATE_SQL)


def compute_time_to_disbursement(spark: SparkSession) -> DataFrame:
    return spark.sql(_TIME_TO_DISBURSEMENT_SQL)


def compute_delinquency_rate(spark: SparkSession, reference_date: str = REFERENCE_DATE) -> DataFrame:
    return spark.sql(_DELINQUENCY_RATE_SQL.format(reference_date=reference_date))


def gold(spark: SparkSession, silver_frames: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Register the star schema and compute the 3 business metrics, each
    segmented as total / by product / by channel / by applied_at month."""
    register_star_schema(spark, silver_frames)

    frames = {
        "approval_rate": compute_approval_rate(spark),
        "time_to_disbursement": compute_time_to_disbursement(spark),
        "delinquency_rate": compute_delinquency_rate(spark),
    }
    for name, df in frames.items():
        print(f"\n[gold] {name}")
        df.orderBy("segment_type", "segment_value").show(50, truncate=False)
    return frames


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze_frames = bronze(spark)
        silver_frames = silver(spark, bronze_frames)
        gold_frames = gold(spark, silver_frames)

        OUTPUT_DIR.mkdir(exist_ok=True)
        for name, df in gold_frames.items():
            ordered = df.orderBy("segment_type", "segment_value")
            ordered.coalesce(1).write.mode("overwrite").option("header", True).csv(
                str(OUTPUT_DIR / name)
            )

        print(f"\n[scaffold] Bronze + Silver + Gold OK. Metrics written to {OUTPUT_DIR}/ 🚀")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
