import json


def read_input_parametes(input_file_path: str):
    """
    Reads input parameters from a JSON file and extracts specific values.

    Parameters:
        input_file_path (str): Path to the JSON file containing input parameters.

    Returns:
        tuple: A tuple containing:
            - source_bookings_data_path (str or None): Path to the source bookings data, if specified.
            - start_date (str or None): Start date for processing, if specified.
            - end_date (str or None): End date for processing, if specified.

    Exceptions:
        FileNotFoundError: Raised if the specified input file is not found.
        Exception: Catches and prints any other unexpected errors.
    """
    try:
        with open(input_file_path, "r") as f:
            input_parameters = json.load(f)
        source_bookings_data_path = input_parameters.get("source_bookings_data_path")
        start_date = input_parameters.get("start_date")
        end_date = input_parameters.get("end_date")
        return source_bookings_data_path, start_date, end_date
    except FileNotFoundError:
        print(f"File : {input_file_path} was not found.")
    except Exception as e:
        print(f"Error occurred: {e}")
