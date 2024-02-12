import pyodbc
from flask import Flask

app = Flask(__name__)
# Cadena de conexión ODBC
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

# Conectarse a la base de datos
conexion = pyodbc.connect(conn_str)

# Crear un cursor
cursor = conexion.cursor()

# Ejecutar una consulta
cursor.execute("SELECT TOP 10  * FROM globant.hired_employees")

# Obtener los resultados
resultados = cursor.fetchall()

# Imprimir los resultados
for fila in resultados:
    print(fila)

# Cerrar la conexión
conexion.close()


@app.route('/api/hola_mundo', methods=['GET'])
def hola_mundo():
    return 'HOLA MUNDO'

if __name__ == '__main__':
    # Ejecutar la aplicación Flask
    app.run(debug=True)

