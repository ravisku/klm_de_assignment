FROM bitnami/spark:latest

# Set working directory
WORKDIR /app

# Install required Python libraries
RUN pip3 install pyspark py4j

# Copy all necessary files and directories
COPY . /app/

# Set default command to run scripts sequentially
CMD python scripts/ingest_airports_data.py && \
    python scripts/ingest_bookings_data.py && \
    python scripts/top_destinations.py