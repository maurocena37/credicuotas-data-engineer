from src.pipeline import clean_loan_applications

APPLICATIONS_SCHEMA = (
    "application_id string, customer_id string, applied_at string, product string, "
    "channel string, requested_amount string, term_months string, credit_score string, "
    "status string"
)


def _row(app_id="APP_1", amount="150000"):
    return (app_id, "CUST_1", "2025-02-16", "PERSONAL", "WEB", amount, "12", "700", "APPROVED")


def test_exact_duplicate_rows_are_dropped(spark):
    df = spark.createDataFrame([_row(), _row()], APPLICATIONS_SCHEMA)
    assert clean_loan_applications(df).count() == 1


def test_types_numeric_and_date_columns(spark):
    df = spark.createDataFrame([_row()], APPLICATIONS_SCHEMA)
    result = clean_loan_applications(df).collect()[0]
    assert str(result["applied_at"]) == "2025-02-16"
    assert result["requested_amount"] == 150000.0
    assert result["term_months"] == 12
    assert result["credit_score"] == 700


def test_negative_requested_amount_is_nulled_and_flagged(spark):
    df = spark.createDataFrame([_row(amount="-500000")], APPLICATIONS_SCHEMA)
    result = clean_loan_applications(df).collect()[0]
    assert result["requested_amount"] is None
    assert result["is_requested_amount_invalid"] is True


def test_null_requested_amount_is_flagged(spark):
    df = spark.createDataFrame([_row(amount=None)], APPLICATIONS_SCHEMA)
    result = clean_loan_applications(df).collect()[0]
    assert result["requested_amount"] is None
    assert result["is_requested_amount_invalid"] is True


def test_valid_requested_amount_is_not_flagged(spark):
    df = spark.createDataFrame([_row(amount="150000")], APPLICATIONS_SCHEMA)
    result = clean_loan_applications(df).collect()[0]
    assert result["is_requested_amount_invalid"] is False


def test_row_is_kept_even_when_amount_is_invalid(spark):
    """A bad amount shouldn't cost us the whole application — status/product
    /channel are still needed for the Approval Rate metric."""
    df = spark.createDataFrame([_row(amount="-500000")], APPLICATIONS_SCHEMA)
    result = clean_loan_applications(df)
    assert result.count() == 1
    assert result.collect()[0]["status"] == "APPROVED"
