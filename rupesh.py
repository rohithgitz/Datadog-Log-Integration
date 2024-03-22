import json
import time
import pyodbc
import requests
 
# Datadog API keys and query details
datadog_api_key = '2d5aa834b783e13d3b31d4eb722ea172'
datadog_app_key = 'b6661f078b18955cd5c3f671e176e15419ca8ec6'
from_time = "now-1440m"
to_time = "now"

# Database configuration
db_config = {
    'server': '10.1.12.14',
    'database': 'PowerBI_Stage',
    'user': 'pp_sveerla',
    'password': '!Welcome@2023SV$$',
    'driver': '{ODBC Driver 17 for SQL Server}',
    'table': 'Completed_Tasks'  
}

# Connect to the database
conn = pyodbc.connect(
    f"DRIVER={db_config['driver']};SERVER={db_config['server']};DATABASE={db_config['database']};UID={db_config['user']};PWD={db_config['password']}"
)
cursor = conn.cursor()

task_names = cursor.execute(f"SELECT JobName FROM JobTask where ScheduleType like '%Task%'").fetchall()

# task_names = ['Accuro Attachment Import', 'Accuro Zero Pay print EORs', 'Task Scheduler task: "CareWorks Attachment Import"', 'Task Scheduler task: "CareWorks UR Determination Attachment Import"', 'IMO-CompIQ Attachment Import', 'MedSource Attachment Import', 'CompanyNurseImport', 'CompanyNurseImport', 'TPMG New Claim Setup', 'TPMG New Claim Setup Response', 'BART - import from BART', 'BART - import from BART', '99 - Get HR Data', 'AC Transit HR import', 'Franco Signor Gould & Lamb Verisk iComply Export', 'Franco Signor Gould & Lamb Verisk iComply Import', 'Gain Life Jarvis export', 'Jarvis - Gain Life - Get Documents', 'ABF-Origami Encrypt-Send Files', 'CJPIA - Send claims data to Origami (ICRMA)', 'CountyofSanMateo move files', 'County of San Mateo - Import Files', '99 Ventiv transfer', 'CJPIA - Send claims data to Ventiv', 'CJPIA - Send ITD claims data to Ventiv - Quarterly', 'Verisk Upload Daily Files', 'Send Safety National monthly', 'VPay - check EOR files'' existence', 'VPay - prepare file', 'VPay - send file', 'VPay - get and process return file', 'Echo - Data Dimension transfer', 'Echo - Data Dimension RETRIEVE settlement', 'CareWorks UR Determination Attachment Import']  # Add all task names here
task_query = " OR ".join([f'"{name}"' for name in task_names])

#print(task_query)

#datadog_query = 'message:"Task Scheduler" service:(Prod-AWS) host:(AWSPrd-Tools OR AWSPrd-DB01)'
datadog_query = f'message:"Task Scheduler" service:(Prod-AWS) host:(AWSPrd-Tools OR AWSPrd-DB01) AND ({task_query})'
 
 
# Datadog API endpoint
url = "https://api.us3.datadoghq.com/api/v2/logs/events/search"
 
# Request headers
headers = {
    "Content-Type": "application/json",
    "DD-API-KEY": datadog_api_key,
    "DD-APPLICATION-KEY": datadog_app_key
}
 
# Request body
payload = {
    "filter": {
        "from": from_time,
        "to": to_time,
        "query": datadog_query
    },
    "page": {"limit": 5000}
}
 


db_config2 = {
    'server': '10.1.12.14',
    'database': 'PowerBI_Stage',
    'user': 'pp_sveerla',
    'password': '!Welcome@2023SV$$',
    'driver': '{ODBC Driver 17 for SQL Server}',
    'table': 'Completed_Tasks_List_Datadog'  
}
 
try:
    # Connect to the database
    conn = pyodbc.connect(
        f"DRIVER={db_config['driver']};SERVER={db_config['server']};DATABASE={db_config['database']};UID={db_config['user']};PWD={db_config['password']}"
    )
    cursor = conn.cursor()

    conn2 = pyodbc.connect(
        f"DRIVER={db_config2['driver']};SERVER={db_config2['server']};DATABASE={db_config2['database']};UID={db_config2['user']};PWD={db_config2['password']}"
    )
    cursor2 = conn2.cursor()
 
    # Send POST request to Datadog API
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()  # Raise an exception for non-successful HTTP status codes
 
    # Parse response JSON
    response_data = response.json()
 
    # Extract logs from response
    all_logs = response_data.get('data', [])

    print("Number of logs retrieved:", len(all_logs))
 
    # Insert data into the database
    for log in all_logs:
        try:
            # Extract log attributes
            event_id = log.get('id')
            event_type = log.get('attributes', {}).get('attributes', {}).get('Event', {}).get('System', {}).get('TimeCreated', {}).get('SystemTime')
            service = log.get('attributes', {}).get('status')
            TaskName = log.get('attributes', {}).get('attributes', {}).get('Event', {}).get('EventData', {}).get('Data', {}).get('TaskName')

            if TaskName:
                TaskName = TaskName.split('\\')[-1].strip()

            # Check if any of the values are None, if so, skip inserting this log
            if None in (event_id, event_type, service, TaskName):
                print("Skipping log due to missing values:", log)
                continue
 
            cursor.execute(
                f"INSERT INTO {db_config['table']} (Task_Name, Last_Run_Time, Last_Task_Result) VALUES (?, ?, ?)",
                (TaskName, event_type, service)
            )
           # print("Log inserted into database successfully:", log)
        except Exception as e:
            print(f"Error inserting log into database: {e}")
            print("Problematic log:", log)
            continue
 
    # Commit the transaction
    conn.commit()
 
    print("All logs retrieved successfully and saved to the database")

# Executing additional SQL script
    additional_sql_script = '''
;WITH TaskCte AS (
    SELECT 
        Task_Name,
        Last_Task_Result,
        Last_Run_Time,
        ROW_NUMBER() OVER(PARTITION BY Task_Name, LEFT(LAst_Run_Time,10) ORDER BY Last_run_Time DESC) AS RowNo
    FROM Completed_Tasks
)

INSERT INTO Completed_Tasks_List_Datadog (Task_Name, Last_Run_Time, Last_Task_Result)
SELECT 
    SUBSTRING(Task_Name, CHARINDEX('\', Task_Name, CHARINDEX('\', Task_Name) + 1) + 1, LEN(Task_Name)) AS TaskName,
    LEFT(Last_Run_Time, 10) AS Last_Run_Time,
    CASE WHEN Last_Task_Result = 'Info' THEN 1 ELSE 0 END AS Last_Task_Result
FROM TaskCte
WHERE RowNo = 1
ORDER BY Task_Name
    '''

    cursor2.execute(additional_sql_script)
    conn2.commit()
 
except Exception as e:
    print(f"An unexpected error occurred: {e}")
 
finally:
    # Close database connection
    if 'conn' in locals():
        conn.close()