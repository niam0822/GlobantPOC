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



@app.route('/RequirementOne', methods=['GET'])

def RequirementOne():
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("""  
                       WITH Temp AS (
                        SELECT 
                            DISTINCT department, 
                            job, 
                            datetime 
                        FROM 
                            globant.hired_employees e 
                            JOIN globant.jobs j ON e.job_id = j.id 
                            JOIN globant.departments d ON e.department_id = d.id 
                        WHERE 
                            YEAR(
                            CONVERT(datetime, datetime)
                            ) = 2021
                        ) 
                        SELECT 
                        Department, 
                        Job, 
                        ISNULL([Q1], 0) AS Q1, 
                        ISNULL([Q2], 0) AS Q2, 
                        ISNULL([Q3], 0) AS Q3, 
                        ISNULL([Q4], 0) AS Q4 
                        FROM 
                        (
                            SELECT 
                            Department, 
                            Job, 
                            CASE WHEN DATEPART(QUARTER, datetime) = 1 THEN 'Q1' WHEN DATEPART(QUARTER, datetime) = 2 THEN 'Q2' WHEN DATEPART(QUARTER, datetime) = 3 THEN 'Q3' WHEN DATEPART(QUARTER, datetime) = 4 THEN 'Q4' END AS Quarter 
                            FROM 
                            Temp
                        ) AS SourceTable PIVOT (
                            COUNT(Quarter) FOR Quarter IN (Q1, Q2, Q3, Q4)
                        ) AS PivotQuarter 
                        ORDER BY 
                        Department, 
                        job ASC
                        """)
        resultado = cursor.fetchall()
        for fila in resultado:
            print(fila) 
        
        # tabla HTML
        table_html = "<table border='1'><tr>"
        # Encabezados de la tabla
        for column in cursor.description:
            table_html += "<th>" + column[0] + "</th>"
        table_html += "</tr>"
        
        # Llenar la tabla
        for fila in resultado:
            table_html += "<tr>"
            for dato in fila:
                table_html += "<td>" + str(dato) + "</td>"
            table_html += "</tr>"
        table_html += "</table>"
        
        return table_html
        
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/RequirementTwo', methods=['GET'])

def RequirementTwo():
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("""  
                       SELECT 
                            departments.id AS department_id,
                            departments.department AS department_name,
                            COUNT(hired_employees.id) AS num_employees_hired
                        FROM 
                            globant.hired_employees
                        JOIN 
                            globant.departments ON hired_employees.department_id = departments.id
                        WHERE 
                            hired_employees.datetime LIKE '2021-%'
                        GROUP BY 
                            departments.id, departments.department
                        HAVING 
                            COUNT(hired_employees.id) > (
                                SELECT 
                                    AVG(num_employees_hired)
                                FROM (
                                    SELECT 
                                        COUNT(hired_employees.id) AS num_employees_hired
                                    FROM 
                                        globant.hired_employees
                                    WHERE 
                                        hired_employees.datetime LIKE '2021-%'
                                    GROUP BY 
                                        department_id
                                ) AS avg_employees
                            )
                        ORDER BY 
                            COUNT(hired_employees.id) DESC
                        """)
        resultado = cursor.fetchall()
        for fila in resultado:
            print(fila) 
        
        #  tabla HTML
        table_html = "<table border='1'><tr>"
        # Encabezados de la tabla
        for column in cursor.description:
            table_html += "<th>" + column[0] + "</th>"
        table_html += "</tr>"
        
        # Llenar la tabla
        for fila in resultado:
            table_html += "<tr>"
            for dato in fila:
                table_html += "<td>" + str(dato) + "</td>"
            table_html += "</tr>"
        table_html += "</table>"
        
        return table_html
        
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Ejecutar la aplicación Flask
    app.run(debug=True)

