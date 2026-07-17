from pyspark.sql import functions as F

from src.pipeline import register_star_schema, compute_delinquency_rate

APPLICATIONS_SCHEMA = "application_id string, product string, channel string, applied_at string, status string"
DISBURSEMENTS_SCHEMA = "disbursement_id string, application_id string"
INSTALLMENTS_SCHEMA = "installment_id string, disbursement_id string, due_date string, paid_at string"

REFERENCE_DATE = "2026-01-15"


def _make_silver_frames(spark):
    apps = spark.createDataFrame(
        [
            ("APP_1", "P1", "C1", "2025-01-01", "APPROVED"),
            ("APP_2", "P2", "C2", "2025-02-01", "APPROVED"),
        ],
        APPLICATIONS_SCHEMA,
    ).withColumn("applied_at", F.to_date("applied_at", "yyyy-MM-dd"))

    disb = spark.createDataFrame(
        [("DISB_1", "APP_1"), ("DISB_2", "APP_2")],
        DISBURSEMENTS_SCHEMA,
    )

    # P1/C1 (via DISB_1): I1 unpaid & due before cutoff -> delinquent
    #                     I2 paid AFTER cutoff -> delinquent (still open at cutoff)
    #                     I3 due AFTER cutoff (not yet due) -> excluded entirely
    # P2/C2 (via DISB_2): I4 paid BEFORE cutoff -> NOT delinquent
    #                     I5 unpaid & due before cutoff -> delinquent
    inst = spark.createDataFrame(
        [
            ("I1", "DISB_1", "2026-01-01", None),
            ("I2", "DISB_1", "2026-01-10", "2026-01-20"),
            ("I3", "DISB_1", "2026-02-01", None),
            ("I4", "DISB_2", "2025-12-01", "2025-12-05"),
            ("I5", "DISB_2", "2025-12-15", None),
        ],
        INSTALLMENTS_SCHEMA,
    )
    inst = inst.withColumn("due_date", F.to_date("due_date", "yyyy-MM-dd")).withColumn(
        "paid_at", F.to_date("paid_at", "yyyy-MM-dd")
    )

    return {
        "customers": spark.createDataFrame([], "customer_id string"),
        "loan_applications": apps,
        "disbursements": disb,
        "installments": inst,
    }


def _rows_by_segment(df):
    return {(r["segment_type"], r["segment_value"]): r for r in df.collect()}


def test_not_yet_due_installment_is_excluded(spark):
    """I3 (due 2026-02-01, after the 2026-01-15 cutoff) must not appear in
    either the numerator or the denominator."""
    register_star_schema(spark, _make_silver_frames(spark))
    total = _rows_by_segment(compute_delinquency_rate(spark, REFERENCE_DATE))[("total", "ALL")]
    assert total["due_count"] == 4  # I1, I2, I4, I5 - not I3


def test_paid_after_cutoff_still_counts_as_delinquent(spark):
    """I2 has a non-null paid_at, but it's after the cutoff - at the cutoff
    date itself the installment was still open, so it must count."""
    register_star_schema(spark, _make_silver_frames(spark))
    total = _rows_by_segment(compute_delinquency_rate(spark, REFERENCE_DATE))[("total", "ALL")]
    assert total["delinquent_count"] == 3  # I1, I2, I5
    assert total["delinquency_rate"] == 0.75


def test_paid_before_cutoff_is_not_delinquent(spark):
    """I4 was fully paid before the cutoff - must not be flagged."""
    register_star_schema(spark, _make_silver_frames(spark))
    p2 = _rows_by_segment(compute_delinquency_rate(spark, REFERENCE_DATE))[("product", "P2")]
    assert p2["due_count"] == 2  # I4, I5
    assert p2["delinquent_count"] == 1  # only I5
    assert p2["delinquency_rate"] == 0.5


def test_segmented_by_product_and_channel(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_delinquency_rate(spark, REFERENCE_DATE))
    assert rows[("product", "P1")]["delinquency_rate"] == 1.0  # I1, I2 both delinquent
    assert rows[("channel", "C1")]["delinquency_rate"] == 1.0
    assert rows[("channel", "C2")]["delinquency_rate"] == 0.5


def test_segmented_by_applied_month(spark):
    register_star_schema(spark, _make_silver_frames(spark))
    rows = _rows_by_segment(compute_delinquency_rate(spark, REFERENCE_DATE))
    assert rows[("applied_month", "2025-01-01")]["delinquency_rate"] == 1.0
    assert rows[("applied_month", "2025-02-01")]["delinquency_rate"] == 0.5
