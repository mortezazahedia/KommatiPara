import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from project_utils.spark_utils import(
    process_data,
    remove_pii,
    remove_credit_card,
    rename_columns,
    validate_email_format
)

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("test_app").getOrCreate()

def test_remove_pii(spark_session):
    data = [(1, "John", "Doe", "john.doe@example.com", "USA")]
    df = spark_session.createDataFrame(data, ["id", "first_name", "last_name", "email", "country"])

    processed_data = remove_pii(df)
    assert "name" not in processed_data.columns
    assert "address" not in processed_data.columns
    assert "phone" not in processed_data.columns


def test_remove_credit_card(spark_session):
    data = [(1, "btc_a_value", "cc_t_value", "cc_n_value")]
    df = spark_session.createDataFrame(data, ["id", "btc_a", "cc_t", "cc_n"])

    processed_data = remove_credit_card(df)
    assert "cc_n" not in processed_data.columns


def test_process_data(spark_session):
    # Define test data
    clients_data = [
        (1, "John", "Doe", "john.doe@example.com", "USA"),
        (2, "Jane", "Smith", "jane.smith@example.com", "UK"),
        # Add more test data as needed
    ]

    financials_data = [
        (1, "btc_address_1", "visa", "1234"),
        (2, "btc_address_2", "mastercard", "5678"),
        # Add more test data as needed
    ]

    expected_data = [
        (1, "John", "Doe", "john.doe@example.com", "USA", "btc_address_1", "visa"),
        (2, "Jane", "Smith", "jane.smith@example.com", "UK", "btc_address_2", "mastercard"),
        # Add expected data based on your transformations
    ]

    # Create Spark DataFrames from test data
    clients_df = spark_session.createDataFrame(clients_data, ["id", "first_name", "last_name", "email", "country"])
    financials_df = spark_session.createDataFrame(financials_data, ["id", "btc_a", "cc_t", "cc_n"])

    # Execute the function to be tested
    result_df = process_data(spark_session, clients_df, financials_df, ["USA", "UK"])

    # Rename columns
    column_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }

    result_df = rename_columns(result_df, column_mapping)
    result_df_count=result_df.count()

    # Create Spark DataFrame from expected data
    expected_df = spark_session.createDataFrame(expected_data, ["client_identifier", "first_name", "last_name", "email", "country", "bitcoin_address", "credit_card_type"])
    expected_df_count=expected_df.count()

    assert result_df_count == expected_df_count
    assert_df_equality(result_df, expected_df)


def test_validate_email_format(spark_session):
    # Create Spark DataFrame for testing
    data = [("John", "john.doe@example.com"),
            ("Jane", "jane.smith@example.com")]

    columns = ["name", "email"]
    df = spark_session.createDataFrame(data, columns)
    validate_email_format(df, "email")
    expected_df = df
    assert_df_equality(df, expected_df)
