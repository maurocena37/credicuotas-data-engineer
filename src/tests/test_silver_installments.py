from src.pipeline import clean_installments

INSTALLMENTS_SCHEMA = (
    "installment_id string, disbursement_id string, installment_number string, "
    "due_date string, due_amount string, paid_at string, paid_amount string"
)
DISB_IDS_SCHEMA = "disbursement_id string"


def _row(inst_id="INST_1", disb_id="DISB_1", paid_at="2025-03-20", paid_amount="57945.0"):
    return (inst_id, disb_id, "1", "2025-03-22", "57945.0", paid_at, paid_amount)


def test_types_dates_and_amounts(spark):
    valid_disbs = spark.createDataFrame([("DISB_1",)], DISB_IDS_SCHEMA)
    df = spark.createDataFrame([_row()], INSTALLMENTS_SCHEMA)
    clean, _ = clean_installments(df, valid_disbs)
    result = clean.collect()[0]
    assert str(result["due_date"]) == "2025-03-22"
    assert str(result["paid_at"]) == "2025-03-20"
    assert result["due_amount"] == 57945.0
    assert result["paid_amount"] == 57945.0


def test_unpaid_installment_is_flagged_not_paid(spark):
    valid_disbs = spark.createDataFrame([("DISB_1",)], DISB_IDS_SCHEMA)
    df = spark.createDataFrame([_row(paid_at=None, paid_amount=None)], INSTALLMENTS_SCHEMA)
    result = clean_installments(df, valid_disbs)[0].collect()[0]
    assert result["is_paid"] is False
    assert result["paid_at"] is None


def test_paid_installment_is_flagged_paid(spark):
    valid_disbs = spark.createDataFrame([("DISB_1",)], DISB_IDS_SCHEMA)
    df = spark.createDataFrame([_row()], INSTALLMENTS_SCHEMA)
    result = clean_installments(df, valid_disbs)[0].collect()[0]
    assert result["is_paid"] is True


def test_orphan_disbursement_id_is_quarantined_not_dropped(spark):
    valid_disbs = spark.createDataFrame([("DISB_999",)], DISB_IDS_SCHEMA)
    df = spark.createDataFrame([_row(disb_id="DISB_1")], INSTALLMENTS_SCHEMA)

    clean, orphans = clean_installments(df, valid_disbs)

    assert clean.count() == 0
    assert orphans.count() == 1
    assert orphans.collect()[0]["installment_id"] == "INST_1"


def test_duplicate_installment_id_is_deduplicated(spark):
    valid_disbs = spark.createDataFrame([("DISB_1",)], DISB_IDS_SCHEMA)
    df = spark.createDataFrame([_row(), _row()], INSTALLMENTS_SCHEMA)
    clean, _ = clean_installments(df, valid_disbs)
    assert clean.count() == 1
