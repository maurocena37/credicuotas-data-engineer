from src.pipeline import clean_customers

CUSTOMERS_SCHEMA = "customer_id string, created_at string, birth_date string, province string, segment string"


def test_types_dates_correctly(spark):
    df = spark.createDataFrame(
        [("CUST_1", "2020-01-15", "1990-05-20", "Buenos Aires", "MASS")],
        CUSTOMERS_SCHEMA,
    )
    result = clean_customers(df).collect()[0]
    assert str(result["created_at"]) == "2020-01-15"
    assert str(result["birth_date"]) == "1990-05-20"


def test_null_province_is_imputed_and_flagged(spark):
    df = spark.createDataFrame(
        [
            ("CUST_1", "2020-01-15", "1990-05-20", None, "MASS"),
            ("CUST_2", "2020-01-15", "1990-05-20", "Chaco", "MASS"),
        ],
        CUSTOMERS_SCHEMA,
    )
    result = {r["customer_id"]: r for r in clean_customers(df).collect()}

    assert result["CUST_1"]["province"] == "UNKNOWN"
    assert result["CUST_1"]["is_province_imputed"] is True

    assert result["CUST_2"]["province"] == "Chaco"
    assert result["CUST_2"]["is_province_imputed"] is False


def test_duplicate_customer_id_is_deduplicated(spark):
    df = spark.createDataFrame(
        [
            ("CUST_1", "2020-01-15", "1990-05-20", "Chaco", "MASS"),
            ("CUST_1", "2020-01-15", "1990-05-20", "Chaco", "MASS"),
        ],
        CUSTOMERS_SCHEMA,
    )
    assert clean_customers(df).count() == 1
