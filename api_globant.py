from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List

# Database setup
# Configuracion de la base de datos
# Usamos sqlite para la creacion de esta base de datos
DATABASE_URL = "sqlite:///./test.db"  
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Se definen 3 tablas
# En la tabla Employee se crean las ForeignKey para relacionar empleados con trabajos y departamentos
class Department(Base):
    __tablename__ = "departments"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)

class Employee(Base):
    __tablename__ = "hired_employees"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    job_id = Column(Integer, ForeignKey("jobs.id"))
    department_id = Column(Integer, ForeignKey("departments.id"))

# Creacion de las tablas en la base de datos 
Base.metadata.create_all(bind=engine)

# Creación de la Aplicación FastAPI
app = FastAPI()

# Endpoint para subir un archivo CSV
def insert_data_from_csv(file, table_name, db_session):
    df = pd.read_csv(file.file, delimiter=',')
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

@app.post("/upload_csv/")
async def upload_csv(table: str, file: UploadFile = File(...)):
    if table not in ["departments", "jobs", "employees"]:
        raise HTTPException(status_code=400, detail="Invalid table name")
    db = SessionLocal()
    try:
        insert_data_from_csv(file, table, db)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
    return {"message": "File processed successfully"}

# Endpoint para insertar datos en batch
def insert_batch_data(table_name, data_list, db_session):
    df = pd.DataFrame(data_list)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

@app.post("/insert_batch/")
async def insert_batch(table: str, data: List[dict]):
    if len(data) > 1000:
        raise HTTPException(status_code=400, detail="Batch limit exceeded (max: 1000)")
    if table not in ["departments", "jobs", "employees"]:
        raise HTTPException(status_code=400, detail="Invalid table name")
    db = SessionLocal()
    try:
        insert_batch_data(table, data, db)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
    return {"message": "Batch inserted successfully"}
