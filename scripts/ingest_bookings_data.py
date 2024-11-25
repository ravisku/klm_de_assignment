import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    input_file_name,
    regexp_extract,
    to_date,
)

from helpers.common_utils import read_input_parametes

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
logger.info("Initializing Spark session for processing bookings events data.")
spark = SparkSession.builder.appName(
    "Ingest bookings events data from raw source file"
).getOrCreate()

# Source and destination paths for data
input_file_path = "input_parameters.json"  # Path to the JSON file with input parameters
cleaned_bookings_data_path = "output/bookings/"  # Path to save cleaned bookings data
corrupted_bookings_data_path = (
    "output/corrupted/bookings/"  # Path to save corrupted records
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


def get_partition_column(df):
    """
    Extracts `arrival_date` in yyyy-MM-dd format from the `arrival_datetime` column.

    Parameters:
        df (DataFrame): Input DataFrame containing raw data.

    Returns:
        DataFrame: DataFrame with an added `arrival_date` column derived from `arrival_datetime`.
    """
    logger.info("Extracting `arrival_date` column from `arrival_datetime`.")
    return df.withColumn(
        "arrival_date", to_date(col("arrival_datetime").substr(1, 10), "yyyy-MM-dd")
    )


def extract_and_transform_data(df):
    """
    Extracts and flattens required fields from the bookings events data.

    Workflow:
        - Explodes nested `passengersList` and `productsList` arrays.
        - Selects key columns for downstream processing.

    Parameters:
        df (DataFrame): Input DataFrame loaded from source JSON files.

    Returns:
        DataFrame: Transformed DataFrame with selected and flattened fields.
    """
    logger.info("Extracting and transforming data from bookings events.")
    return df.select(
        col("timestamp").alias("event_timestamp"),
        explode(col("event.DataElement.travelrecord.passengersList")).alias(
            "passenger"
        ),
        explode(col("event.DataElement.travelrecord.productsList")).alias("product"),
    ).select(
        col("event_timestamp"),
        col("passenger.uci").alias("passenger_uci"),
        col("passenger.age").alias("passenger_age"),
        col("passenger.passengerType").alias("passenger_type"),
        col("product.bookingStatus").alias("booking_status"),
        col("product.flight.operatingAirline").alias("operating_airline"),
        col("product.flight.originAirport").alias("origin_airport"),
        col("product.flight.destinationAirport").alias("destination_airport"),
        col("product.flight.departureDate").alias("departure_datetime"),
        col("product.flight.arrivalDate").alias("arrival_datetime"),
    )


def ingest_bookings_data(
    source_bookings_data_path, cleaned_bookings_data_path, corrupted_bookings_data_path
):
    """
    Ingests raw bookings events data, applies transformations, and saves it in parquet format.

    Workflow:
        1. Read raw JSON data from the source path.
        2. Filter and save corrupted records to a separate path.
        3. Apply transformations:
            - Flatten nested structures.
            - Add derived columns (e.g., `arrival_date`).
            - Remove duplicate rows.
        4. Save cleaned data to the output path in Parquet format.

    Parameters:
        source_bookings_data_path (str): Path to the source JSON files.
        cleaned_bookings_data_path (str): Path to save the cleaned and transformed parquet files.
        corrupted_bookings_data_path (str): Path to save corrupted records.

    Raises:
        Exception: Logs and raises any errors during the ingestion process.
    """
    try:
        logger.info("Starting the bookings data ingestion process.")

        # Read source JSON files dynamically inferring schema
        logger.info(f"Reading data from source path: {source_bookings_data_path}")
        df = spark.read.option("mode", "PERMISSIVE").json(source_bookings_data_path)

        # Handle corrupted records
        if "_corrupt_record" in df.columns:
            corrupted_records = df.filter(col("_corrupt_record").isNotNull())
            corrupted_records.write.mode("overwrite").csv(
                corrupted_bookings_data_path, header=True
            )
            logger.info(f"Corrupted records saved to {corrupted_bookings_data_path}.")
            df = df.filter(col("_corrupt_record").isNull())

        # Apply transformations
        logger.info("Applying transformations to the data.")
        df = extract_and_transform_data(df)  # Flatten and extract required fields
        df = drop_duplicates(df)  # Remove duplicate rows
        df = get_partition_column(df)  # Add `arrival_date` column
        df = df.withColumn(
            "execution_datetime", current_timestamp()
        )  # Add metadata column
        logger.info(f"Transformed data record count: {df.count()}")

        # Write the cleaned data to the destination path, partitioned by `arrival_date`
        logger.info(f"Writing transformed data to: {cleaned_bookings_data_path}")
        df.repartition(1).write.mode("overwrite").partitionBy("arrival_date").parquet(
            cleaned_bookings_data_path
        )
        logger.info(
            "Bookings data ingestion and transformation completed successfully."
        )
    except Exception as e:
        logger.error(f"Error during bookings data ingestion: {e}")
        raise


if __name__ == "__main__":
    """
    Main execution flow:
        1. Reads input parameters from a JSON file.
        2. Runs the ETL process for ingesting and transforming bookings events data.
        3. Handles errors and logs success or failure messages.
    """
    try:
        # Read input parameters to get source file location
        logger.info("Reading input parameters.")
        source_bookings_data_path = read_input_parametes(input_file_path)[0]

        # Run the ingestion process
        logger.info("Starting the ETL process for bookings events data.")
        ingest_bookings_data(
            source_bookings_data_path,
            cleaned_bookings_data_path,
            corrupted_bookings_data_path,
        )
        logger.info("ETL process for bookings events data completed successfully.")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
