# Globant POC

El proyecto busca por medio de un API conectar a una base de datos que se encuentra en Azure y cargar infromación mientras se valida la calidad de la misma en el proceso.
También busca al final hacer dos consultas a la base de datos y dar respuesta por un endpoint y por un Dashboard en PowerBI.

### Pre-requisitos 📋

- Se necesita una base de datos, no necesariamente en Azure, solo una base de datos funcional, que no sea NOSQL,  a la que tengas acceso y tener los datos de conexión.
- Crear las siguientes tablas que se usan ddurante la ejecución del desarrollo:

CREATE TABLE globant.departments (
    id INT PRIMARY KEY,
    department_name VARCHAR(255)
);

CREATE TABLE globant.jobs (
    id INT PRIMARY KEY,
    job_title VARCHAR(255)
);

CREATE TABLE globant.hired_employees (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    datetime VARCHAR(255),
    department_id INT,
    job_id INT,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id)
);

- Postman para hacer el envío de archivos por medio del servicio de la API
- Tener PowerBI Desktop para visualizar el dashboard al final del proyecto.

## Ejecutando  ⚙️

Para ejecutar el proyecto usar el comando "python.exe" sobre el archivo GlobantPOC.py, esto habilitara que puedas entrar a al API localmente y las URL's que se usan en el proyecto son:
- http://127.0.0.1:5000/Load_Data
- http://127.0.0.1:5000/RequirementOne
- http://127.0.0.1:5000/RequirementTwo

Para realizar el cargue de archivos puedes usar POSTMAN y usando la primer URL hacer la petición con los archivos. 


### Analisis de resultados 🔩

En el caso del primer servicio "Load_Data", al final de la ejecución del código, si finalizó de forma exitosa, va a enviar el mensaje "Cargue de archivos terminado correctamente". Se puede confirmar entrando a la Base de Datos y consultando las tablas destino.
En caso de los otros dos servicios, la pagina te mostrará una tabla con la información consultada directamente. 

### PowerBI 📄
Cuando ya el código este corriendo, puedes abrir el archivo Power BI para ver la información que se encuentra actualmente en la Base de Datos, mostrada en dos Dashboards.


## Autores ✒️


* **Cristian Ibarra** - *Ingeniero de Telecomunicaciones* 

