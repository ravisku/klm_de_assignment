# KLM Data Engineering Assignment

This repository contains a PySpark-based application designed to process and analyze bookings and airports data. The application is containerized using Docker, making it easy to run locally or scale for production use.

---

## Table of Contents
1. [Overview](#overview)
2. [Features](#features)
3. [Technologies Used](#technologies-used)
4. [Getting Started](#getting-started)
5. [Running Locally](#running-locally)
6. [Scaling for Production](#scaling-for-production)
7. [Repository Structure](#repository-structure)
8. [Scripts Overview](#scripts-overview)

---

## Overview

This project demonstrates a data engineering solution for processing bookings and airports datasets. The application ingests raw JSON and CSV files, performs necessary transformations, and writes the results in a clean format suitable for analysis.

---

## Features

- Data ingestion and transformation using PySpark.
- Containerized application for portability.
- Handles corrupted records gracefully.
- Outputs results in Parquet format, partitioned for efficient querying.
- Configurable input parameters via a JSON file.

---

## Technologies Used

- **PySpark**: For scalable data processing.
- **Docker**: For containerization.
- **Python**: Core programming language.
- **JSON**: Used for configuration and input data format.

---

## Getting Started

### Prerequisites

- Install [Docker](https://docs.docker.com/get-docker/).

### Clone the Repository

```bash
git clone https://github.com/ravisku/klm_de_assignment.git
cd klm_de_assignment
```

## Running Locally

To run the application on your local machine:

### 1. Build the Docker Image:
```bash
docker build -t klm .
```
### 2. Run the Application:
```bash
docker run -v /app -v $(pwd):/app klm
```

## Explanation

- `-v $(pwd):/app`: Mounts the current working directory to `/app` in the container, enabling the application to write results locally.

## Results

- The transformed and processed data will be saved in the `output/` directory in your local repository.

---

## Scaling for Production

Running a PySpark application in a container offers flexibility and portability. However, handling terabytes of data requires a distributed computing infrastructure. Below are strategies to scale this application for production use:

### Deploying on a Spark Cluster

PySpark is designed to run in a distributed manner. You can deploy this containerized application to a Spark cluster to handle large-scale workloads.

#### Cluster Options

- **Kubernetes (K8s)**:
  - Use a Kubernetes cluster with a Spark operator to manage your PySpark application.
  - Each pod in the Kubernetes cluster acts as an executor, distributing the workload.

- **YARN**:
  - Submit the containerized application to a Hadoop YARN cluster.
  - The container serves as the driver program, and executors run on worker nodes.

- **Cloud Platforms**:
  - Leverage managed Spark services such as:
    - Amazon EMR/Glue
    - Google Dataproc

  - These platforms handle resource allocation and cluster management for us. We can submit your application as a job to these services.

## Repository Structure
```
klm_de_assignment/
│
├── data/                   # Input data files (e.g., bookings, airports)
├── output/                 # Processed data output
├── scripts/                # pyspark scripts for data processing
│   ├── ingest_airports_data.py
│   ├── ingest_bookings_data.py
│   ├── top_destinations.py
│   ├── top_destinations.sql
│
├── helpers/                # Utility functions and modules
│   └── common_utils.py
│
├── input_parameters.json   # Configuration file for the application
├── Dockerfile              # Dockerfile to build the application container
├── README.md               # Project documentation 
```

## Scripts Overview

### 1. `scripts/ingest_airports_data.py`
This script:
- Reads raw airport data from a CSV file.
- Cleans the data by:
  - Removing duplicate records.
  - Replacing placeholder null values (e.g., `\N`) with actual nulls.
- Adds metadata like `execution_datetime`.
- Saves the cleaned data as a Parquet file.

---

### 2. `scripts/ingest_bookings_data.py`
This script:
- Reads raw bookings data from a JSON file.
- Handles corrupted records by:
  - Filtering invalid rows into a separate file.
- Extracts and flattens nested fields such as passengers and products.
- Derives additional columns like `arrival_date` for partitioning.
- Cleans the data by:
  - Removing duplicate records.
  - Adding metadata like `execution_datetime`.
- Saves the cleaned data as a Parquet file, partitioned by `arrival_date`.

---

### 3. `scripts/top_destinations.py`
This script:
- Reads the processed bookings and airports data.
- Performs transformations and aggregations to identify top destinations:
  - Groups data by destination, season, and day of the week.
  - Counts unique passengers for each destination.
- Outputs the results as a CSV file in the `output/results` directory.
