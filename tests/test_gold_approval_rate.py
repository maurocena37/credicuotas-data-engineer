from pyspark.sql import functions as F

from src.pipeline import register_star_schema, compute_approval_rate

APPLICATIONS_SCHEMA = "application_id string, product string, channel string, applied_at string, status string"


def _apps(spark):
    # total: 4 rows, 2 approved -> 0.5
    # product P1: 3 rows (1 approved, 1 rejected, 1 pending) -> 1/3
    # product P2: 1 row (1 approved) -> 1.0
    # channel C1: 2 rows (1 approved) -> 0.5
    # channel C2: 2 rows (1 approved) -> 0.5
    # applied_month 2025-01: 2 rows (1 approved) -> 0.5
    # applied_month 2025-02: 2 rows (1 approved) -> 0.5
    df = spark.createDataFrame(
        [
            ("APP_1", "P1", "C1", "2025-01-10", "APPROVED"),
            ("APP_2", "P1", "C1", "2025-01-15", "REJECTED"),
            ("APP_3", "P1", "C2", "2025-02-01", "PENDING"),
            ("APP_4", "P2", "C2", "2025-02-05", "APPROVED"),
        ],
        APPLICATIONS_SCHEMA,
    )
    return df.withColumn("applied_at", F.to_date("applied_at", "yyyy-MM-dd"))


def _make_silver_frames(spark):
    return {
        "customers": spark.createDataFrame([], "customer_id string"),
        "loan_applications": _apps(spark),
        "disbursements": spark.createDataFrame([], "disbursement_id string, application_id string"),
        "installments": spark.createDataFrame([], "installment_id string, disbursement_id string"),
    }


def _rows_by_segment(df):
    return {(r["segment_type"], r["segment_value"]): r for r in df.collect()}


def test_pending_counts_in_denominator(spark):
    """Confirmed design decision: PENDING must count toward total_count so the
    metric for a closed period doesn't retroactively change."""
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_approval_rate(spark))
    total = rows[("total", "ALL")]
    assert total["total_count"] == 4
    assert total["approved_count"] == 2
    assert total["approval_rate"] == 0.5


def test_segmented_by_product(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_approval_rate(spark))
    assert rows[("product", "P1")]["total_count"] == 3
    assert rows[("product", "P1")]["approved_count"] == 1
    assert rows[("product", "P2")]["total_count"] == 1
    assert rows[("product", "P2")]["approval_rate"] == 1.0


def test_segmented_by_channel(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_approval_rate(spark))
    assert rows[("channel", "C1")]["total_count"] == 2
    assert rows[("channel", "C2")]["total_count"] == 2
    assert rows[("channel", "C1")]["approval_rate"] == 0.5


def test_segmented_by_applied_month(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_approval_rate(spark))
    assert str(rows[("applied_month", "2025-01-01")]["total_count"]) == "2"
    assert str(rows[("applied_month", "2025-02-01")]["total_count"]) == "2"


def test_all_four_segment_types_present(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    segment_types = {r["segment_type"] for r in compute_approval_rate(spark).collect()}
    assert segment_types == {"total", "product", "channel", "applied_month"}
