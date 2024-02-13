from flask import Flask, request,jsonify
import pyodbc
import csv
from io import StringIO
import logging



app = Flask(__name__)

logging.basicConfig(filename='error.log', level=logging.ERROR)

# Cadena de conexión 
conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:globant-server.database.windows.net,1433;"
    "Database=db-globant;"
    "Uid=globant;"
    "Pwd=Cris0822;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

#Estructuras de las tablas que recibiran la información 

estructura_tabla_hired_employees = [
    ("id", "INTEGER"),
    ("name", "STRING"),
    ("datetime", "STRING"),
    ("department_id", "INTEGER"),
    ("job_id", "INTEGER")
]

estructura_tabla_departments = [
    ("id", "INTEGER"),
    ("department", "STRING")
]

estructura_tabla_jobs = [
    ("id", "INTEGER"),
    ("job", "STRING")
]

# Función para validar tipo da datos

def validar_datos_tabla(row, tipo):
    try:
        for encabezado, campo in enumerate(row):
            tipo_validacion = tipo[encabezado][1]
            if tipo_validacion == "INTEGER":
                if not str(campo).isdigit():
                    return False
            elif tipo_validacion == "STRING":
                if not isinstance(campo, str) or len(campo) < 1:
                    return False
        return True
    except IndexError:
        return False

def LoadDataHiredEmployee(conn_hired_employees):
    # Recibir el archivo 
        print("Leyendo archivo hired_employees")
        csv_hired_employees = request.files['hired_employees']
        
        # Leer el archivo 
        csv_content_hired_employees = csv_hired_employees.read().decode('utf-8')
        csv_buffer_hired_employees = StringIO(csv_content_hired_employees)

        # Conexión a la Base de Datos 
        cursor_hired_employees = conn_hired_employees.cursor()

        # Leer el archivo CSV e insertar los datos en la tabla hired_employees
        csv_reader_hired_employees = csv.reader(csv_buffer_hired_employees)

        batch_size = 0

        for row in csv_reader_hired_employees:

            # Lanzar función para validar
            if validar_datos_tabla(row,estructura_tabla_hired_employees):
                # Insert a la tabla hired_employees
                cursor_hired_employees.execute("INSERT INTO globant.hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4]))
            
                # Contar el número de filas, para hacer commit, cada 1000 filas
                batch_size += 1
                
                if batch_size == 1000:
                    conn_hired_employees.commit()
                    batch_size = 0
                
            else:
                # Enviar al Log la fila que no pasó la validación de datos
                logging.error(f"Fila no válida en la tabla de Hired Employees: {row}")
        
        print("Insert terminado correctamente para la tabla Hired Employees")
        
        conn_hired_employees.commit()
        
        
        
def LoadDataDepartments(conn_departments):
        
        print("Leyendo archivo Deparments")
        csv_departments = request.files['departments']
        # Leer el archivo 
        csv_content_departments = csv_departments.read().decode('utf-8')
        csv_buffer_departments = StringIO(csv_content_departments)

        # Conexión a la Base de Datos 
        conn_departments = pyodbc.connect(conn_str)
        cursor_departments = conn_departments.cursor()

        # Leer el archivo CSV e insertar los datos en la tabla departments
        csv_reader_departments = csv.reader(csv_buffer_departments)

        batch_size = 0    

        for row in csv_reader_departments:

            # Lanzar función para validar
            if validar_datos_tabla(row,estructura_tabla_departments):
                # Insert a la tabla departments
                cursor_departments.execute("INSERT INTO globant.departments (id, department) VALUES (?, ?)", (row[0], row[1]))
            
                # Contar el número de filas, para hacer commit, cada 1000 filas
                batch_size += 1
                
                if batch_size == 1000:
                    conn_departments.commit()
                    batch_size = 0
                
            else:
                # Enviar al Log la fila que no pasó la validación de datos
                logging.error(f"Fila no válida en la tabla de Departments: {row}")
        
        print("Insert terminado correctamente para la tabla departments")
        
        conn_departments.commit()

def LoadDatajobs(conn_jobs):
        
        print("Leyendo archivo Jobs")
        csv_jobs = request.files['jobs']
        # Leer el archivo 
        csv_content_jobs = csv_jobs.read().decode('utf-8')
        csv_buffer_jobs = StringIO(csv_content_jobs)

        # Conexión a la Base de Datos 
        conn_jobs = pyodbc.connect(conn_str)
        cursor_jobs = conn_jobs.cursor()

        # Leer el archivo CSV e insertar los datos en la tabla jobs
        csv_reader_jobs = csv.reader(csv_buffer_jobs)

        batch_size = 0    

        for row in csv_reader_jobs:

            # Lanzar función para validar
            if validar_datos_tabla(row,estructura_tabla_jobs):
                # Insert a la tabla jobs
                cursor_jobs.execute("INSERT INTO globant.jobs (id, job) VALUES (?, ?)", (row[0], row[1]))
            
                # Contar el número de filas, para hacer commit, cada 1000 filas
                batch_size += 1
                
                if batch_size == 1000:
                    conn_jobs.commit()
                    batch_size = 0
                
            else:
                # Enviar al Log la fila que no pasó la validación de datos
                logging.error(f"Fila no válida en la tabla de jobs: {row}")
        
        print("Insert terminado correctamente para la tabla jobs")

        conn_jobs.commit()


@app.route('/Load_Data', methods=['POST'])
def load_hired_employees():
    try:
        conn = pyodbc.connect(conn_str)

        LoadDataHiredEmployee(conn)
        LoadDataDepartments(conn)
        LoadDatajobs(conn)
        # cerrar la conexión a la BD
        conn.close()

        print("Cargue de archivos terminado correctamente")
        
        return "Cargue terminado correctamente"
    
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Ejecutar la aplicación Flask
    app.run(debug=True)

