# pruba_tecnica_globant

REST API for DB Migration

Description

This project contains a FastAPI based REST API to do the migration of data from CSV files to a SQL database.
It allows:
1. Receive historical data from CSV files
2. Upload files to the new DB
3. Insert batch transactions (1 up to 1000 rows) with one request

Installation

Clone the repository:

git clone https://github.com/yourusername/yourrepo.git

cd yourrepo

Create a virtual environment and install dependencies:

python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt

Run the application:

uvicorn main:app --reload

Technologies

FastAPI: For creating the REST API.
Pandas: For processing CSV files.
SQLAlchemy: ORM for database interaction.
SQLite (default): Can be replaced with PostgreSQL or MySQL.

API Endpoints

- Cargar los archivos CSV
Endpoint: POST /upload_csv/
Parameters:
1.table (query param) - Name of the target table (departments, jobs, employees).
2.file (form-data) - CSV file (comma-separated).

- Batch Insert Data
Endpoint: POST /insert_batch/
Parameters:
1.table (query param) - Name of the target table.
2.data (JSON body) - List of up to 1000 dictionary entries.


