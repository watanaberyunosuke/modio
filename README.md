# Mod IO Test

This repository contains two main components:

1. **Spark ETL Script** (`script.py`): A PySpark script for an ETL (Extract, Transform, Load) process, specifically designed to handle mod download logs.

2. **Unit Tests** (`test_script.py`): Unit tests for the Spark ETL script, ensuring the correctness of specific functions.

## Spark ETL Script (`script.py`)

### Overview

The ETL script is responsible for processing mod download logs. It reads JSON formatted logs, extracts relevant information, performs transformations, and finally writes the processed data to a specified output path.

### Requirements

- Python 3.x, the code is built and tested on Python 3.9
- PySpark
- Access to a Spark cluster, local or distributed, the code is designed for local

### Key Components

- `extract_gameid_modid_udf`: A User Defined Function (UDF) for extracting game and mod IDs.
- `create_spark_session`: Function to create a Spark session.
- `process_mod_downloads`: The main ETL function, which processes the logs.
- `clean_up`: Utility function to clean up input data.

### How to Run

1. Ensure PySpark and all dependencies are installed.
2. Place your input JSON files in the `./input` directory.
3. Run the script using the command: `python script.py`.

## Unit Tests (`test_script.py`)

### Overview

The unit test script provides test cases for the functions defined in the ETL script. It tests the functionality of the UDF and the overall ETL process.

### Requirements

- Python 3.x
- PySpark
- Delta-Python
- unittest (Python's built-in module)

### Key Components

- `SparkETLTests`: A test class containing all unit tests.
- `test_extract_gameid_modid_udf`: Tests the `extract_gameid_modid_udf` function.
- `test_process_mod_downloads`: Placeholder for testing the `process_mod_downloads` function (to be implemented).

### How to Run

1. Run the unit tests using the command: `python -m unittest test_script.py`.

## Notes

- Make sure to adjust paths and configurations according to your environment.
- For production use, review and test the code thoroughly.
- The unit test for `process_mod_downloads` needs to be implemented based on the specific logic of the ETL process.

---
