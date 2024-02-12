from flask import Flask, request,jsonify
import pyodbc
import csv
from io import StringIO
import logging



app = Flask(__name__)
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

# Función para validar tipo da datos

def validar_hired_employees(row):
    
    try:
        # Convertir los datos según las estructuras
        id = int(row[0])
        name = row[1]
        datetime_val = row[2]
        department_id = int(row[3])
        job_id = int(row[4])
        
        # Verificar si los tipo de datos coinciden 
        if not str(id).isdigit():
            return False
        if len(name)<1:
            return False
        if len(datetime_val)<1:
            return False
        # Verificar que el department_id y job_id sean enteros
        if not (str(department_id).isdigit() and str(job_id).isdigit()):
            return False
        if not (str(job_id).isdigit()):
            return False
        
        # Si todos coinciden enviar True
        return True

    except ValueError:
        # Si falla en alguno enviar False
        return False


@app.route('/hired_employees', methods=['POST'])
def nuevos_empleados_csv():
    try:
        # Recibir el archivo 
        print("Se va a leer el archivo")
        csv_file = request.files['file']
        
        # Leer el archivo 
        csv_content = csv_file.read().decode('utf-8')
        csv_buffer = StringIO(csv_content)

        # Conexión a la Base de Datos 
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Leer el archivo CSV e insertar los datos en la tabla hired_employees
        csv_reader = csv.reader(csv_buffer)
    
        for row in csv_reader:

            # Lanzar función para validar
            if validar_hired_employees(row):    
                # Insert a la tabla hired_employees
                cursor.execute("INSERT INTO globant.hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4]))
            else:
                # Enviar al Log la fila que no pasó la validación de datos
                logging.error(f"Fila no válida: {row}")
                

        # cerrar la conexión a la BD
        conn.commit()
        conn.close()

        print("Insert terminado correctamente")
        return "Insert terminado correctamente"
    
    except Exception as e:
        # Registrar el error en el archivo de registro de errores
        logging.error(f"Error en el insert de la tabla hired_employees: {str(e)}")
        print(f"Error en el insert de la tabla hired_employees: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Ejecutar la aplicación Flask
    app.run(debug=True)

