"""End-to-end check: bronze -> silver against the actual data/ CSVs.

These numbers were confirmed by profiling the raw files directly (pandas,
outside Spark) and are pinned here as regression guards: if a future change
to silver() accidentally stops catching one of these issues, one of these
asserts will fail.
"""
from src.pipeline import bronze, silver


def test_silver_against_real_data(spark):
    bronze_frames = bronze(spark)
    silver_frames = silver(spark, bronze_frames)

    # loan_applications: 810 raw rows, 10 exact duplicates -> 800 unique
    assert silver_frames["loan_applications"].count() == 800

    # 15 null + 9 negative requested_amount = 24 invalid rows in the raw file,
    # but one of them (APP_000715, a null-amount row) is also one of the 10
    # exact-duplicate pairs, so after dedup only 23 unique rows remain flagged.
    invalid_amount_count = (
        silver_frames["loan_applications"]
        .filter("is_requested_amount_invalid = true")
        .count()
    )
    assert invalid_amount_count == 23

    # customers: 12 rows had a null province, all imputed to UNKNOWN
    imputed_province_count = (
        silver_frames["customers"].filter("is_province_imputed = true").count()
    )
    assert imputed_province_count == 12

    # disbursements: 5 rows point at an application_id that doesn't exist
    assert silver_frames["_quarantine_orphan_disbursements"].count() == 5
    assert silver_frames["disbursements"].count() == 377 - 5

    # all 12 dd/MM/yyyy rows parsed to a non-null date (same as the 12
    # iso-format-only rows would have) - i.e. no date silently became null
    null_dates = silver_frames["disbursements"].filter("disbursed_at is null").count()
    assert null_dates == 0

    # installments: no orphans in the raw data, but any installment whose
    # disbursement got quarantined above must be quarantined too
    assert silver_frames["installments"].count() + silver_frames[
        "_quarantine_orphan_installments"
    ].count() == 4407
