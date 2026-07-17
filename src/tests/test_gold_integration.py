"""End-to-end check: bronze -> silver -> gold against the actual data/ CSVs.

Numbers were captured by running the full pipeline against the real data and
are pinned here as regression guards, plus structural invariants (segment
breakdowns must sum back to the total) that would catch a broken GROUP BY
regardless of the exact numbers.
"""
from src.pipeline import bronze, silver, gold


def _rows_by_segment(df):
    return {(r["segment_type"], r["segment_value"]): r for r in df.collect()}


def test_gold_against_real_data(spark):
    bronze_frames = bronze(spark)
    silver_frames = silver(spark, bronze_frames)
    gold_frames = gold(spark, silver_frames)

    approval = _rows_by_segment(gold_frames["approval_rate"])
    ttd = _rows_by_segment(gold_frames["time_to_disbursement"])
    delinquency = _rows_by_segment(gold_frames["delinquency_rate"])

    # --- pinned totals (regression guard) ---
    assert approval[("total", "ALL")]["total_count"] == 800
    assert approval[("total", "ALL")]["approved_count"] == 391

    assert ttd[("total", "ALL")]["disbursed_count"] == 372

    assert delinquency[("total", "ALL")]["due_count"] == 1808
    assert delinquency[("total", "ALL")]["delinquent_count"] == 207

    # --- structural invariant: product/channel breakdowns must sum to total ---
    product_rows = [r for (t, _), r in approval.items() if t == "product"]
    assert sum(r["total_count"] for r in product_rows) == approval[("total", "ALL")]["total_count"]

    channel_rows = [r for (t, _), r in approval.items() if t == "channel"]
    assert sum(r["total_count"] for r in channel_rows) == approval[("total", "ALL")]["total_count"]

    ttd_product_rows = [r for (t, _), r in ttd.items() if t == "product"]
    assert sum(r["disbursed_count"] for r in ttd_product_rows) == ttd[("total", "ALL")]["disbursed_count"]

    delinquency_channel_rows = [r for (t, _), r in delinquency.items() if t == "channel"]
    assert sum(r["due_count"] for r in delinquency_channel_rows) == delinquency[("total", "ALL")]["due_count"]

    # --- sanity bounds: every rate must be a valid proportion ---
    for r in approval.values():
        assert 0.0 <= r["approval_rate"] <= 1.0
    for r in delinquency.values():
        assert 0.0 <= r["delinquency_rate"] <= 1.0
    for r in ttd.values():
        assert r["avg_days_to_disbursement"] > 0
