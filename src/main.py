# pragma: no cover
import sys
import logging
import argparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
sys.path.append('.')

from project_utils.logging_config import configure_logger
from project_utils.spark_utils import create_spark_session, load_data, process_data, save_and_log_processed_data, \
    validate_file_path, rename_columns


def main():  # pragma: no cover

    if len(sys.argv) != 4:
        logging.error("Usage: main.py <clients_path> <financials_path> <countries>")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Process client data.")
    parser.add_argument("client_file", type=validate_file_path, help="Path to the client data file")
    parser.add_argument("financial_file", type=validate_file_path, help="Path to the financial data file")
    parser.add_argument("countries", type=str, help="List of countries to filter (comma-separated)")

    args = parser.parse_args()

    client_file_path = args.client_file
    financial_file_path = args.financial_file
    countries = args.countries.split(',')

    # Now client_file_path and financial_file_path are validated file paths
    print("Client file path:", client_file_path)
    print("Financial file path:", financial_file_path)
    print("Countries:", countries)

    # Initialize Spark session
    spark = create_spark_session()

    # Define schemas for clients and financials
    clients_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True)
    ])

    financials_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("btc_a", StringType(), True),
        StructField("cc_t", StringType(), True),
        StructField("cc_n", StringType(), True)
    ])

    # Process data
    clients_df = load_data(spark, client_file_path, clients_schema)
    financials_df = load_data(spark, financial_file_path, financials_schema)

    processed_data = process_data(spark, clients_df, financials_df, countries)

    # Rename columns
    column_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }

    renamed_data = rename_columns(processed_data, column_mapping)

    # Save the processed data
    save_and_log_processed_data(renamed_data, output_format="csv")


if __name__ == "__main__":
    configure_logger()
    main()
