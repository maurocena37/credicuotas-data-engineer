from pyspark.sql import functions as F

from src.pipeline import register_star_schema, compute_time_to_disbursement

APPLICATIONS_SCHEMA = "application_id string, product string, channel string, applied_at string, status string"
DISBURSEMENTS_SCHEMA = "disbursement_id string, application_id string, disbursed_at string"


def _make_silver_frames(spark):
    apps = spark.createDataFrame(
        [
            # P1/C1: applied 2025-01-01 -> disbursed 2025-01-05 = 4 days
            ("APP_1", "P1", "C1", "2025-01-01", "APPROVED"),
            # P1/C1: applied 2025-01-10 -> disbursed 2025-01-20 = 10 days
            ("APP_2", "P1", "C1", "2025-01-10", "APPROVED"),
            # P2/C2: applied 2025-02-01 -> disbursed 2025-02-11 = 10 days
            ("APP_3", "P2", "C2", "2025-02-01", "APPROVED"),
            # never disbursed - must be excluded entirely (inner join)
            ("APP_4", "P2", "C2", "2025-02-02", "REJECTED"),
        ],
        APPLICATIONS_SCHEMA,
    ).withColumn("applied_at", F.to_date("applied_at", "yyyy-MM-dd"))

    disb = spark.createDataFrame(
        [
            ("DISB_1", "APP_1", "2025-01-05"),
            ("DISB_2", "APP_2", "2025-01-20"),
            ("DISB_3", "APP_3", "2025-02-11"),
        ],
        DISBURSEMENTS_SCHEMA,
    ).withColumn("disbursed_at", F.to_date("disbursed_at", "yyyy-MM-dd"))

    return {
        "customers": spark.createDataFrame([], "customer_id string"),
        "loan_applications": apps,
        "disbursements": disb,
        "installments": spark.createDataFrame([], "installment_id string, disbursement_id string"),
    }


def _rows_by_segment(df):
    return {(r["segment_type"], r["segment_value"]): r for r in df.collect()}


def test_total_average_days(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_time_to_disbursement(spark))
    total = rows[("total", "ALL")]
    assert total["disbursed_count"] == 3
    assert total["avg_days_to_disbursement"] == 8.0  # (4+10+10)/3


def test_rejected_application_is_excluded(spark):
    """APP_4 was never disbursed - the inner join to fact_disbursement must
    drop it, not produce a null days_to_disbursement."""
    register_star_schema(spark, _make_silver_frames(spark))
    total = _rows_by_segment(compute_time_to_disbursement(spark))[("total", "ALL")]
    assert total["disbursed_count"] == 3


def test_segmented_by_product(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_time_to_disbursement(spark))
    assert rows[("product", "P1")]["avg_days_to_disbursement"] == 7.0  # (4+10)/2
    assert rows[("product", "P2")]["avg_days_to_disbursement"] == 10.0


def test_segmented_by_channel(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_time_to_disbursement(spark))
    assert rows[("channel", "C1")]["avg_days_to_disbursement"] == 7.0
    assert rows[("channel", "C2")]["avg_days_to_disbursement"] == 10.0


def test_segmented_by_applied_month(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_time_to_disbursement(spark))
    assert rows[("applied_month", "2025-01-01")]["avg_days_to_disbursement"] == 7.0
    assert rows[("applied_month", "2025-02-01")]["avg_days_to_disbursement"] == 10.0
