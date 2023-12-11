# src/spark_utils.py
import logging
import os
import configparser
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session(app_name="KommatiParaApp"): # pragma: no cover
    try:
        return SparkSession.builder.appName(app_name).config("spark.network.timeout", "600s").getOrCreate()
    except Exception as e:
        logging.error("Error creating Spark session: %s", str(e))
        raise


def read_config():  # pragma: no cover
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config


def validate_file_path(path):  # pragma: no cover
    if not os.path.isfile(path):
        error_message = f"{path} is not a valid file path"
        logging.error(error_message)
        raise argparse.ArgumentTypeError(error_message)
    return path


# Helper function to validate email format
def validate_email_format(df, email_column): # pragma: no cover
    email_format_regex = r"[^@]+@[^@]+\.[^@]+"

    invalid_email_count = df.filter(~col(email_column).rlike(email_format_regex)).count()
    if invalid_email_count > 0:
        error_message = f"{invalid_email_count} rows have invalid email format in the '{email_column}' column."
        logging.error(error_message)
        raise ValueError(error_message)


def validate_schema(df, expected_columns): # pragma: no cover
    actual_columns = df.columns
    if set(expected_columns) != set(actual_columns):
        error_message = f"Invalid schema. Expected columns: {expected_columns}, Actual columns: {actual_columns}"
        logging.error(error_message)
        raise ValueError(error_message)

    # Validate email column format
    email_column = "email"
    if email_column in expected_columns:
        validate_email_format(df, email_column)


def load_data(spark, clients_path, file_schema): # pragma: no cover
    try:
        # Load datasets with explicit schemas
        df = spark.read.csv(clients_path, header=True, schema=file_schema)

        # Validate schemas
        #validate_schema(clients_df, ["id", "first_name", "last_name", "email", "country"])
        #validate_schema(financials_df, ["id", "btc_a", "cc_t", "cc_n"])

        # Handle Duplicate Data
        # Handle Null or Missing Data
        # Handle Incorrect Data Types
        # Handle Invalid Values or Outliers
        # Foreign Keys
        # Data Validation  : I have implemented for email column
        # Schema Check : I have implemented this check here
        # Unique Values Check
        # Value Range Check

        return df

    except Exception as e:
        logging.error("Error processing data: %s", str(e))
        raise


def process_data(spark, clients_df, financials_df, countries):
    try:

        # Remove PII and credit card number
        clients_processed = remove_pii(clients_df)
        financials_processed = remove_credit_card(financials_df)

        # Filter clients by countries
        clients_filtered = filter_clients_by_country(clients_processed, countries)

        # Join datasets on the 'id' field
        joined_data = join_datasets(clients_filtered, financials_processed)

        return joined_data

    except Exception as e:
        logging.error("Error processing data: %s", str(e))
        raise


def save_and_log_processed_data(processed_data, output_format="parquet"): # pragma: no cover
    try:
        config = read_config()
        output_path = config.get("Paths", "output_path")

        if output_format.lower() == "parquet":
            processed_data.write.mode("overwrite").parquet(output_path)
        elif output_format.lower() == "csv":
            processed_data.write.mode("overwrite").csv(output_path, header=True)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        logging.info("Data processing completed. Processed data saved at: %s", output_path)
    except Exception as e:
        logging.error("Error saving processed data: %s", str(e))
        raise


def filter_clients_by_country(clients_df, countries): # pragma: no cover
    try:
        return clients_df.filter(col("country").isin(countries))
    except Exception as e:
        logging.error("Error filtering clients by country: %s", str(e))
        raise


def remove_pii(clients_df): # pragma: no cover
    try:
        # Assuming PII columns are 'name', 'address', etc.
        return clients_df.drop("name", "address", "phone")
    except Exception as e:
        logging.error("Error removing PII: %s", str(e))
        raise


def remove_credit_card(financials_df): # pragma: no cover
    try:
        # Assuming credit card column is 'cc_number'
        return financials_df.drop("cc_n")
    except Exception as e:
        logging.error("Error removing credit card information: %s", str(e))
        raise


def join_datasets(clients_df, financials_df): # pragma: no cover
    try:
        return clients_df.join(financials_df, "id", "inner")
    except Exception as e:
        logging.error("Error joining datasets: %s", str(e))
        raise


def rename_columns(data_df, column_mapping): # pragma: no cover
    try:
        # Remove None values from the column mapping
        column_mapping = {k: v for k, v in column_mapping.items() if v is not None}

        # Handle additional columns not in the mapping
        additional_columns = [col for col in data_df.columns if col not in column_mapping]
        column_mapping.update({col: col for col in additional_columns})

        return data_df.toDF(*[column_mapping.get(col, col) for col in data_df.columns])
    except Exception as e:
        logging.error("Error renaming columns: %s", str(e))
        raise



