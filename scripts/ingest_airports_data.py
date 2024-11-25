import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
logger.info("Initializing Spark session for processing airports data.")
spark = SparkSession.builder.appName(
    "Ingest airports data from raw source file"
).getOrCreate()

# Source and destination paths for data
source_airports_data_path = (
    "data/airports/airports.csv"  # Path to source CSV file containing raw airport data
)
cleaned_airports_data_path = (
    "output/airports/"  # Path to save cleaned and transformed airport data
)


def drop_duplicates(df):
    """
    Removes duplicate rows from the DataFrame.

    Parameters:
        df (DataFrame): The input DataFrame that may contain duplicate rows.

    Returns:
        DataFrame: A new DataFrame with duplicate rows removed.
    """
    logger.info("Removing duplicate rows from the DataFrame.")
    return df.dropDuplicates()


def format_null_values(df):
    """
    Replaces placeholder null values (e.g., "\\N") with proper nulls.

    Parameters:
        df (DataFrame): Input DataFrame containing raw data.

    Returns:
        DataFrame: A DataFrame with placeholder null values replaced by actual nulls.
    """
    logger.info("Replacing '\\N' with proper null values.")
    return df.replace("\\N", None)


def ingest_airports_data(source_airports_data_path, cleaned_airports_data_path):
    """
    Ingests raw airport data, applies necessary transformations, and saves it in parquet format.

    Workflow:
        1. Reads raw airport data from the source CSV file.
        2. Applies schema definition to structure the data.
        3. Removes duplicate rows.
        4. Replaces placeholder null values (e.g., "\\N") with proper nulls.
        5. Adds an 'execution_datetime' column with the current timestamp.
        6. Writes the cleaned data to the destination path in parquet format.

    Parameters:
        source_airports_data_path (str): Path to the source CSV file containing raw airport data.
        cleaned_airports_data_path (str): Path to save the cleaned and transformed parquet files.

    Raises:
        Exception: Logs and re-raises any exceptions encountered during the process.
    """
    try:
        logger.info("Starting the airports data ingestion process.")

        # Define the schema for the airport data
        airports_schema = StructType(
            [
                StructField("airport_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("IATA", StringType(), True),
                StructField("ICAO", StringType(), True),
                StructField("lat", FloatType(), True),
                StructField("lon", FloatType(), True),
                StructField("alt", IntegerType(), True),
                StructField("timezone", FloatType(), True),
                StructField("DST", StringType(), True),
                StructField("timezone_tz", StringType(), True),
                StructField("type", StringType(), True),
                StructField("source", StringType(), True),
            ]
        )

        # Read source CSV file
        logger.info(f"Reading data from source path: {source_airports_data_path}")
        df = (
            spark.read.format("csv")
            .option(
                "header", "false"
            )  # Indicating the file does not contain a header row
            .schema(airports_schema)  # Apply the defined schema
            .load(source_airports_data_path)
        )

        # Apply transformations
        logger.info("Applying transformations to the data.")
        df = drop_duplicates(df)  # Remove duplicate rows
        df = format_null_values(df)  # Replace placeholder null values
        df = df.withColumn(
            "execution_datetime", current_timestamp()
        )  # Add a column with the current timestamp
        logger.info(f"Number of records after transformation: {df.count()}")

        # Write the cleaned data to the destination path
        logger.info(f"Writing transformed data to: {cleaned_airports_data_path}")
        df.repartition(1).write.mode("overwrite").parquet(
            cleaned_airports_data_path
        )  # Save as parquet
        logger.info(
            "Airports data ingestion and transformation completed successfully."
        )
    except Exception as e:
        logger.error(f"Error during airports data ingestion: {e}")
        raise


if __name__ == "__main__":
    """
    Main execution block:
        - Reads raw airport data from the specified source.
        - Cleans and transforms the data.
        - Saves the cleaned data to the destination path in parquet format.
    """
    logger.info("Starting the ETL process for airports data.")
    ingest_airports_data(source_airports_data_path, cleaned_airports_data_path)
    logger.info("ETL process for airports data completed successfully.")
