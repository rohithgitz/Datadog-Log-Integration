# Project Summary:
This project aims to fetch logs related to specific tasks from Datadog and store them in a SQL Server database for monitoring and analysis purposes. It involves querying Datadog's API to retrieve logs based on task names, parsing the JSON response, and inserting the log data into a database table.

# Your Contributions:

# Configuration Setup:
You configured the Datadog API keys, database connection details, and defined the time range for log retrieval.
# Database Interaction:
You established connections to two separate SQL Server databases and executed SQL queries to retrieve task names and insert log data.
# Datadog Integration:
You constructed a query to filter logs from Datadog based on task names and sent a POST request to the Datadog API to fetch logs.
# Data Processing:
You parsed the JSON response from Datadog to extract relevant log attributes such as event ID, event type, service, and task name.
# Data Insertion:
You inserted the extracted log data into a table in the first database (Completed_Tasks) for storage and further analysis.
# Additional SQL Script: 
You formulated and executed an additional SQL script to process and insert data into another table in the second database (Completed_Tasks_List_Datadog).
# Exception Handling:
You implemented exception handling to manage errors that may occur during the execution of the script, ensuring smooth operation.
# Database Connection Closure:
You closed the database connections after the execution of the script to release resources and maintain efficiency.


# Datadog Log Integration

This project is a Python script designed to retrieve logs from Datadog, extract relevant information about scheduled tasks, and store that information in a SQL Server database. It utilizes the Datadog API for log retrieval and PyODBC for database interaction.

## Features

- Retrieves logs from Datadog using the Datadog API.
- Extracts relevant information about scheduled tasks from the retrieved logs.
- Stores the extracted task information in a SQL Server database.
- Provides error handling and logging for robustness and traceability.

## Prerequisites

Before running the script, ensure you have the following:

- Python 3.x installed on your system.
- Required Python packages installed. You can install them using pip:
pip install pyodbc requests

- Access to Datadog with appropriate API keys.
- Access to a SQL Server database where you intend to store the extracted task information.

## Configuration

Before running the script, you need to configure the following:

1. Datadog API keys: Replace `datadog_api_key` and `datadog_app_key` in the script with your Datadog API keys.
2. Database configuration: Update `db_config` and `db_config2` dictionaries with your SQL Server database connection details.
3. Specify the time range for log retrieval by updating `from_time` and `to_time` variables in the script.
4. Optionally, customize the SQL query for extracting task names if needed.

## Usage

1. Clone this repository to your local machine:

git clone https://github.com/rohithgitz/Datadog-Log-Integration.git



2. Navigate to the project directory:

cd Datadog-Log-Integration

3. Edit the script to configure it according to your requirements (as described in the Configuration section).

4. Run the script:

python datadog_log_integration.py

