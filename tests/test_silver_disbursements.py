from src.pipeline import clean_disbursements

DISBURSEMENTS_SCHEMA = (
    "disbursement_id string, application_id string, disbursed_at string, "
    "disbursed_amount string, annual_interest_rate string, term_months string"
)
APP_IDS_SCHEMA = "application_id string"


def _row(disb_id="DISB_1", app_id="APP_1", disbursed_at="2025-02-20"):
    return (disb_id, app_id, disbursed_at, "150000", "0.55", "12")


def test_iso_date_is_parsed(spark):
    valid_apps = spark.createDataFrame([("APP_1",)], APP_IDS_SCHEMA)
    df = spark.createDataFrame([_row(disbursed_at="2025-02-20")], DISBURSEMENTS_SCHEMA)
    clean, _ = clean_disbursements(df, valid_apps)
    assert str(clean.collect()[0]["disbursed_at"]) == "2025-02-20"


def test_eu_date_format_is_also_parsed(spark):
    """disbursed_at mixes yyyy-MM-dd and dd/MM/yyyy in the raw data."""
    valid_apps = spark.createDataFrame([("APP_1",)], APP_IDS_SCHEMA)
    df = spark.createDataFrame([_row(disbursed_at="14/05/2025")], DISBURSEMENTS_SCHEMA)
    clean, _ = clean_disbursements(df, valid_apps)
    assert str(clean.collect()[0]["disbursed_at"]) == "2025-05-14"


def test_types_numeric_columns(spark):
    valid_apps = spark.createDataFrame([("APP_1",)], APP_IDS_SCHEMA)
    df = spark.createDataFrame([_row()], DISBURSEMENTS_SCHEMA)
    clean, _ = clean_disbursements(df, valid_apps)
    result = clean.collect()[0]
    assert result["disbursed_amount"] == 150000.0
    assert result["annual_interest_rate"] == 0.55
    assert result["term_months"] == 12


def test_orphan_application_id_is_quarantined_not_dropped(spark):
    valid_apps = spark.createDataFrame([("APP_999",)], APP_IDS_SCHEMA)
    df = spark.createDataFrame([_row(app_id="APP_1")], DISBURSEMENTS_SCHEMA)

    clean, orphans = clean_disbursements(df, valid_apps)

    assert clean.count() == 0
    assert orphans.count() == 1
    assert orphans.collect()[0]["disbursement_id"] == "DISB_1"


def test_valid_application_id_passes_through(spark):
    valid_apps = spark.createDataFrame([("APP_1",)], APP_IDS_SCHEMA)
    df = spark.createDataFrame([_row(app_id="APP_1")], DISBURSEMENTS_SCHEMA)

    clean, orphans = clean_disbursements(df, valid_apps)

    assert clean.count() == 1
    assert orphans.count() == 0


def test_duplicate_disbursement_id_is_deduplicated(spark):
    valid_apps = spark.createDataFrame([("APP_1",)], APP_IDS_SCHEMA)
    df = spark.createDataFrame([_row(), _row()], DISBURSEMENTS_SCHEMA)
    clean, _ = clean_disbursements(df, valid_apps)
    assert clean.count() == 1
