#Importo librerias
import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String
from sqlalchemy import text

# Configuración de la base de datos SQLite
DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL)

# Defino la app. FastAPI app
app = FastAPI()

# Ruta donde están los CSV
DATA_PATH = "data/"

#Esta funcion va a definir la estructura (columnas y tipos de datos) de las tablas que van a quedar en la base de datos
def get_dtype(table_name: str) -> dict:
    """
    Retorna un diccionario con los tipos de datos para cada columna según la tabla.
    """
    if table_name == "departments":
        # Estructura: id INTEGER, department STRING
        return {"id": Integer(), "department": String()}
    elif table_name == "jobs":
        # Estructura: id INTEGER, job STRING
        return {"id": Integer(), "job": String()}
    elif table_name == "hired_employees":
        # Estructura: id INTEGER, name STRING, datetime STRING, department_id INTEGER, job_id INTEGER
        return {
            "id": Integer(),
            "name": String(),
            "datetime": String(),
            "department_id": Integer(),
            "job_id": Integer()
        }
    else:
        return {}

#Esta funcion va a cargar los archivos desde mi compu en la unibacion local
def load_csv_to_db():
    """Carga los archivos CSV en la base de datos SQLite en lotes de 1,000 filas.
    Se definen manualmente los nombres de las columnas (ya que los CSV no tienen headlines)
    y se especifican los tipos de datos."""
    try:
        # Verifica si la carpeta data/ existe
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError("La carpeta 'data/' no existe. Crea la carpeta y coloca los archivos CSV dentro.")

        # Cargar departamentos
        df_departments = pd.read_csv(f"{DATA_PATH}departments.csv", header=None, names=["id", "department"])
        insert_in_batches(df_departments, "departments")

        # Cargar trabajos
        df_jobs = pd.read_csv(f"{DATA_PATH}jobs.csv", header=None, names=["id", "job"])
        insert_in_batches(df_jobs, "jobs")

        # Cargar empleados
        df_hired_employees = pd.read_csv(f"{DATA_PATH}hired_employees.csv", header=None, names=["id", "name", "datetime", "department_id", "job_id"])
        insert_in_batches(df_hired_employees, "hired_employees")

        print("✅ Datos cargados correctamente desde los archivos CSV.")

    except Exception as e:
        print(f"❌ Error al cargar los archivos CSV: {e}")

#Esta funcion va a realizar la carga en batch por lotes de 1.000 registros
def insert_in_batches(df, table_name):
    """Inserta los datos en lotes de 1,000 filas.
    Se utiliza el parámetro dtype para definir los tipos de datos."""
    
    try:
        dtype_dict = get_dtype(table_name)
        for i in range(0, len(df), 1000):
            batch = df.iloc[i:i+1000]
            batch.to_sql(table_name, engine, if_exists="append", index=False, dtype=dtype_dict)
        print(f"✅ Datos insertados en la tabla {table_name}.")
    except Exception as e:
        print(f"❌ Error insertando datos en la tabla {table_name}: {e}")


@app.post("/load_data/")
async def load_data():
    """Endpoint para cargar datos desde los archivos CSV en la base de datos en lotes de 1,000."""
    try:
        load_csv_to_db()
        return {"message": "Datos cargados exitosamente desde los archivos CSV."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint para el requerimiento de los trimestres
@app.get("/metrics/quarterly_hires/")
async def quarterly_hires():
    """
    Retorna el número de empleados contratados para cada trabajo y departamento
    en 2021, dividido por trimestre, ordenado alfabéticamente por departamento y trabajo.
    """
    query = """
    SELECT dep.department, job.job,
           SUM(CASE WHEN strftime('%m', empl.datetime) BETWEEN '01' AND '03' THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN strftime('%m', empl.datetime) BETWEEN '04' AND '06' THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN strftime('%m', empl.datetime) BETWEEN '07' AND '09' THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN strftime('%m', empl.datetime) BETWEEN '10' AND '12' THEN 1 ELSE 0 END) AS Q4
    FROM hired_employees empl
    JOIN departments dep 
    ON empl.department_id = dep.id
    JOIN jobs job 
    ON empl.job_id = job.id
    WHERE strftime('%Y', empl.datetime) = '2021'
    GROUP BY dep.department, job.job
    ORDER BY dep.department, job.job;
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = [dict(row) for row in result.mappings()]
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint para el Requerimiento 2
@app.get("/metrics/departments_above_avg/")
async def departments_above_avg():
    """
    Retorna una lista con el id, nombre y número de empleados contratados de cada departamento
    que contrató más empleados que el promedio de contrataciones en 2021 para todos los departamentos,
    ordenados de mayor a menor por el número de empleados contratados.
    """
    query = """
    WITH dept_hires AS (
        SELECT department_id, COUNT(*) AS hires
        FROM hired_employees
        WHERE strftime('%Y', datetime) = '2021'
        GROUP BY department_id
    ),
    avg_hires AS (
        SELECT AVG(hires) AS avg_hires FROM dept_hires
    )
    SELECT dep.id, dep.department, dh.hires
    FROM departments dep
    JOIN dept_hires dh ON dep.id = dh.department_id,
         avg_hires
    WHERE dh.hires > avg_hires.avg_hires
    ORDER BY dh.hires DESC;
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = [dict(row) for row in result.mappings()]
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Ejecutar la carga inicial 
if __name__ == "__main__":
    load_csv_to_db()
