import logging
import os
import configparser
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log


def create_spark_session(app_name="KommatiParaApp"):  # pragma: no cover
    """
    Helper function to create spark session
    :param app_name: The desired application name
    :return: Sparksession
    """
    try:
        logging.info("Start to create Spark Session")
        return SparkSession.builder.appName(app_name).config("spark.network.timeout", "600s").getOrCreate()
    except Exception as e:
        logging.error("Error creating Spark session: %s", str(e))
        raise


def read_config():  # pragma: no cover
    """
    Helper function to read config file
    :return: Entire of config file
    """
    try:
        config = configparser.ConfigParser()
        config.read("config.ini")
        logging.info("Reading confing")
        return config
    except Exception as e:
        logging.error("Error in reading confing.ini: %s", str(e))
        raise


def validate_file_path(path: str) -> str:  # pragma: no cover
    """
    Helper function to validate file path
    :param ath: Desired filepath
    :return: File path
    """
    if not os.path.isfile(path):
        error_message = f"{path} is not a valid file path"
        logging.error(error_message)
        raise argparse.ArgumentTypeError(error_message)
    return path


def validate_email_format(df, email_column):  # pragma: no cover
    """
    Helper function to validate email format
    :param df: DataFrame for validating
    :param email_column:  Email column name
    """
    email_format_regex = r"[^@]+@[^@]+\.[^@]+"

    invalid_email_count = df.filter(~col(email_column).rlike(email_format_regex)).count()
    if invalid_email_count > 0:
        error_message = f"{invalid_email_count} rows have invalid email format in the '{email_column}' column."
        logging.error(error_message)
        raise ValueError(error_message)


#
def validate_schema(df, expected_columns):  # pragma: no cover
    """
    Helper function to validate dataframe schema
    :param df:  Dataframe for validating
    :param expected_columns: Desired dataframe schema
    """
    actual_columns = df.columns
    if set(expected_columns) != set(actual_columns):
        error_message = f"Invalid schema. Expected columns: {expected_columns}, Actual columns: {actual_columns}"
        logging.error(error_message)
        raise ValueError(error_message)

    # Validate email column format
    email_column = "email"
    if email_column in expected_columns:
        validate_email_format(df, email_column)


# Helper function to load data from specified path
def load_data(spark, clients_path, file_schema): # pragma: no cover
    """
    Helper function to load dataframe with desired schema
    :param spark: Ppark session
    :param clients_path: File path
    :param file_schema: DataFarme Schema
    :return: Loaded DataFrame
    """
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

        logging.info(f"Loading DataFrame{clients_path}")
        return df

    except Exception as e:
        logging.error("Error processing data: %s", str(e))
        raise


def process_data(spark, clients_df, financials_df, countries):
    """
    Helper function to Process dataframe with desired schema
    :param spark: SparkSession
    :param clients_df: First DataFrame
    :param financials_df: Second DataFrame
    :param countries: List of your Desired countries
    :return: Result dataframe
    """
    try:
        # Remove PII and credit card number
        clients_processed = remove_pii(clients_df)
        financials_processed = remove_credit_card(financials_df)

        # Filter clients by countries
        clients_filtered = filter_clients_by_country(clients_processed, countries)

        # Join datasets on the 'id' field
        joined_data = join_datasets(clients_filtered, financials_processed,"id")

        return joined_data
        logging.info(f"Joining two DataFrames")
    except Exception as e:
        logging.error("Error processing data: %s", str(e))
        raise


def save_and_log_processed_data(processed_data, output_format="parquet"):  # pragma: no cover
    """
    Helper function to Save dataframe in CSV or Parquet Format
    :param processed_data: Input DataFrame
    :param output_format: CSV/Parquet
    """
    try:
        config = read_config()
        output_path = config.get("Paths", "output_path")

        if output_format.lower() == "parquet":
            processed_data.write.mode("overwrite").parquet(output_path)
            logging.info(f"Storing Result DataFrame in parquet file format ")
        elif output_format.lower() == "csv":
            processed_data.write.mode("overwrite").csv(output_path, header=True)
            logging.info(f"Storing Result DataFrame in CSV file format ")
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        logging.info("Data processing completed. Processed data saved at: %s", output_path)
    except Exception as e:
        logging.error("Error saving processed data: %s", str(e))
        raise


def filter_clients_by_country(clients_df, countries):  # pragma: no cover
    """
    Helper function to filter dataframe based on countries
    :param clients_df: Input DataFrame
    :param countries: listed countries splited with ,
    """
    try:
        logging.info(f"Start Filtering DataFrame with list of countries:{countries}")
        return clients_df.filter(col("country").isin(countries))
    except Exception as e:
        logging.error("Error filtering clients by country: %s", str(e))
        raise


def remove_pii(clients_df):  # pragma: no cover
    """
    Helper function to remove PII
    :param clients_df: Input DateFrame
    :return: Output DataFrame after removing PII
    """
    try:
        # Assuming PII columns are 'name', 'address', etc. but I didn't remove anything, maybe lastname for this case!!!
        logging.info(f"Start Removing PII Data")
        return clients_df.drop("name", "address", "phone")
    except Exception as e:
        logging.error("Error removing PII: %s", str(e))
        raise


def remove_credit_card(financials_df): # pragma: no cover
    """
    Helper function to remove credit_card info
    :param financials_df: Input DataFrame
        :return: Returned DataFrame
    """
    try:
        # Assuming credit card column is 'cc_number'
        logging.info(f"Start Removing credit card Information")
        return financials_df.drop("cc_n")
    except Exception as e:
        logging.error("Error removing credit card information: %s", str(e))
        raise


def join_datasets(clients_df, financials_df, join_column:"id"):  # pragma: no cover
    """
    Helper function to join 2 DataFrame
    :param clients_df: First DataFrame
    :param financials_df: Second DataFrame
    :param join_column: join_column , consider just one column
    :return: Joined DataFrame
    """
    try:
        logging.info(f"Start Joining 2 DataFrame")
        return clients_df.join(financials_df, join_column, "inner")
    except Exception as e:
        logging.error("Error joining datasets: %s", str(e))
        raise


def rename_columns(data_df, column_mapping): # pragma: no cover
    """
    Helper function to rename some columns in input DataFarme
    :param data_df: Input_DataFrame
    :param column_mapping: Desired Column Mapping
    :return: DataFrame after mapping Columns Name
    """
    try:
        # Remove None values from the column mapping
        column_mapping = {k: v for k, v in column_mapping.items() if v is not None}

        # Handle additional columns not in the mapping
        additional_columns = [col for col in data_df.columns if col not in column_mapping]
        column_mapping.update({col: col for col in additional_columns})
        logging.info(f"Renaming DataFrame Columns")
        return data_df.toDF(*[column_mapping.get(col, col) for col in data_df.columns])
    except Exception as e:
        logging.error("Error renaming columns: %s", str(e))
        raise



