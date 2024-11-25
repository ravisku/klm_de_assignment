import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from helpers.common_utils import read_input_parametes

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
logger.info("Initializing Spark session for processing SQL queries.")
spark = SparkSession.builder.appName(
    "Run SQL Query on Bookings Events Data"
).getOrCreate()

# Source and destination paths
input_file_path = "input_parameters.json"
cleaned_bookings_data_path = "output/bookings/"
cleaned_airports_data_path = "output/airports/"
result_output_path = "output/results/"
sql_file_path = "scripts/top_destinations.sql"


def execute_sql_query(sql_file_path, result_output_path):
    """
    Reads an SQL query from a file, executes it, and saves the result to a CSV file.

    Parameters:
        sql_file_path (str): Path to the SQL file.
        result_output_path (str): Path to save the query results as a CSV file.

    Returns:
        None

    Raises:
        Exception: If the SQL query execution or result saving fails.
    """
    try:
        logger.info(f"Loading SQL query from {sql_file_path}.")
        with open(sql_file_path, "r") as file:
            query = file.read()

        # Execute the SQL query
        logger.info("Executing the SQL query.")
        result_df = spark.sql(query)

        # Save the result as a CSV file
        logger.info(f"Saving query results to {result_output_path}.")
        result_df.write.csv(result_output_path, mode="overwrite", header=True)

        logger.info("SQL query executed and results saved successfully.")
    except Exception as e:
        logger.error(f"Failed to execute the SQL query: {e}")
        raise


def read_airports_data(cleaned_airports_data_path):
    """
    Reads the cleaned airports data from a Parquet file and registers it as a temporary view.

    Parameters:
        cleaned_airports_data_path (str): Path to the Parquet file for airports data.

    Returns:
        None

    Raises:
        Exception: If reading the airports data fails.
    """
    try:
        logger.info(f"Reading airports data from {cleaned_airports_data_path}.")
        df_airports = spark.read.parquet(cleaned_airports_data_path)
        df_airports.createOrReplaceTempView("airports")
        logger.info("Airports data registered as a temporary view.")
    except Exception as e:
        logger.error(f"Failed to read airports data: {e}")
        raise


def read_bookings_data(cleaned_bookings_data_path, start_date, end_date):
    """
    Reads the cleaned bookings data from a Parquet file, filters it by arrival_date,
    and registers it as a temporary view.

    Parameters:
        cleaned_bookings_data_path (str): Path to the Parquet file for bookings data.
        start_date (str): Start date for filtering bookings data (inclusive).
        end_date (str): End date for filtering bookings data (inclusive).

    Returns:
        None

    Raises:
        Exception: If reading or filtering bookings data fails.
    """
    try:
        logger.info(f"Reading bookings data from {cleaned_bookings_data_path}.")
        df_bookings = spark.read.parquet(cleaned_bookings_data_path)

        # Filter data based on the arrival_date range
        logger.info(f"Filtering bookings data between {start_date} and {end_date}.")
        df_bookings = df_bookings.filter(
            (col("arrival_date") >= start_date) & (col("arrival_date") <= end_date)
        )

        df_bookings.createOrReplaceTempView("bookings")
        logger.info("Bookings data filtered and registered as a temporary view.")
    except Exception as e:
        logger.error(f"Failed to read or filter bookings data: {e}")
        raise


if __name__ == "__main__":
    """
    Main execution flow:
        1. Read input parameters (start_date and end_date).
        2. Read and register airports data as a temporary view.
        3. Read, filter, and register bookings data as a temporary view.
        4. Execute the SQL query and save the results.
    """
    try:
        logger.info("Starting the ETL process.")

        # Read input parameters
        start_date, end_date = read_input_parametes(input_file_path)[1:3]
        logger.info(f"Start Date: {start_date}, End Date: {end_date}")

        # Read data and register views
        read_airports_data(cleaned_airports_data_path)
        read_bookings_data(cleaned_bookings_data_path, start_date, end_date)

        # Execute SQL query and save results
        execute_sql_query(sql_file_path, result_output_path)

        logger.info("ETL process completed successfully.")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
