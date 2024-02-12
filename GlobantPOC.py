from flask import Flask, request
import pyodbc
import csv
from io import StringIO



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


@app.route('/hired_employees', methods=['POST'])
def nuevos_empleados_csv():
    
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
            
                # Insert a la tabla hired_employees
                cursor.execute("INSERT INTO globant.hired_employees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4]))
        # cerrar la conexión a la BD
        conn.commit()
        conn.close()

        return "Empleados insertados correctamente"


if __name__ == '__main__':
    # Ejecutar la aplicación Flask
    app.run(debug=True)

