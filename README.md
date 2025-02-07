# REST API for DB Migration

# Description

This project contains a FastAPI based REST API to do the migration of data from CSV files to a SQL database.
It allows:
1. Receive historical data from CSV files
2. Upload files to the new DB
3. Insert batch transactions (1 up to 1000 rows) with one request

# Installation

Create a virtual environment and install dependencies:

python -m venv venv
venv\Scripts\activate 
pip install -r requirements.txt

Run the application:

uvicorn api_globant:app --reload

# Technologies

FastAPI: For creating the REST API.
Pandas: For processing CSV files.
SQLAlchemy: ORM for database interaction.
SQLite (default): Can be replaced with PostgreSQL or MySQL.

# API Endpoints

- Cargar los archivos CSV
Endpoint: POST /load_data/
Parameters:
1.table (in the function load_csv_to_db) - Name of the tables (departments, jobs, employees).
2.file (form-data) - CSV file (comma-separated).
3.data (define in teh function insert_in_batches) - List of up to 1000 entries.

- KPI number 1
Endpoint: GET /metrics/quarterly_hires/
Parameters: 
No input parameters are required.


- KPI number 2
Endpoint: GET /metrics/departments_above_avg/
Parameters: 
No input parameters are required.



