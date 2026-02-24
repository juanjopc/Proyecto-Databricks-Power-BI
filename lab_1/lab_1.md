### 📜 1. Librerías
Importamos las funciones de PySpark para transformar datos.
* `from pyspark.sql.functions import *`

### 🏛️ 2. Arquitectura: Creación de Catálogo, Esquemas y Volúmenes
1. Crear el catálogo `Lab_1`.
2. Crear los esquemas `bronze`, `silver` y `gold` dentro del catálogo.
3. Crear el volumen `bronze_vol` dentro del esquema `bronze`.
4. Crear la carpeta `files` dentro del volumen.
5. Cargar manualmente los archivos `patients.csv`, `location.csv` y `appointments.txt` en la carpeta.

### 🥉 3. Esquema Bronze: Creación de Tablas
Tomamos los datos de los archivos y los guardamos como tablas en el esquema `bronze`.
1. Crear la tabla `patients` usando los datos de `patients.csv` *(Opción 1: Usando SQL)*.
2. Crear la tabla `patients` usando los datos de `patients.csv` *(Opción 2: Usando PySpark)*.
3. Crear la tabla `location` usando los datos de `location.csv` *(Usando PySpark)*.
4. Crear la tabla `appointments` usando los datos de `appointments.csv` *(Usando PySpark)*.

### 🥈 4. Esquema Silver: Transformación de Tipos de Datos y Nombres
Tomamos las tablas del esquema `bronze`, aseguramos que los tipos de datos sean correctos, igualamos los nombres de las columnas de cruce y las guardamos en el esquema `silver`.

1. **Tabla `patients`:**
   * `birthdate` (pasa a tipo Fecha) y `locationId` (pasa a tipo Entero).
   * `Id` (pasa a llamarse `patientId`).
   * Guardamos la tabla en el esquema `silver`.
2. **Tabla `appointments`:**
   * `appointment_date` (pasa a tipo Fecha) y `appointment_cost` (pasa a tipo Decimal).
   * `Id` (pasa a llamarse `appointmentId`).
   * Guardamos la tabla en el esquema `silver`.
3. **Tabla `location`:**
   * `Id` (pasa a tipo Entero).
   * `Id` (pasa a llamarse `locationId`).
   * Guardamos la tabla en el esquema `silver`.

### 🥈 5. Esquema Silver: Cruce de Tablas (Join)
1. Unimos pacientes (`patients`) con sus consultas médicas (`appointments`) y su ubicación (`location`). 
*(Nota: Seleccionamos los campos en común una sola vez para evitar columnas duplicadas).*

### 🥈 6. Esquema Silver: Agregar Nuevos Campos
Agregamos nuevos campos a la tabla cruzada:
* **`age`**: Calculamos la edad actual del paciente basándonos en su fecha de nacimiento (`birthdate`) y la fecha actual.
* **`appointment_year`**: Extraemos el año de la fecha de consulta (`appointment_date`).
* **`appointment_cost_tier`**: Categorizamos el costo de la consulta médica (`appointment_cost`) en:
  * 🟢 **low:** menor a $500
  * 🟡 **medium:** entre $500 y $3000
  * 🟠 **high:** entre $3000 y $20000
  * 🔴 **critical:** mayor a $20000

### 🥈 7. Esquema Silver: Limpieza y Calidad de Datos
1. **Filtrado de errores:** Eliminamos filas que cumplan alguna de estas condiciones:
   * `patientId` nulo.
   * `appointmentId` nulo.
   * Identificación `SSN` nula.
   * Edad (`age`) negativa.
   * Costo de consulta (`appointment_cost`) negativo.
2. **Eliminamos consultas médicas duplicadas** (filas donde `patientId`, `appointmentId` y `appointment_date` se repitan al mismo tiempo).
3. Guardamos la tabla limpia como `consolidated` en el esquema `silver`.
4. Guardamos los datos descartados en una tabla llamada `rejected` para futuras auditorías.

### 🥇 8. Esquema Gold: Filtrar los datos
Tomamos la tabla `consolidated` del esquema `silver` y filtramos solo lo necesario para nuestro reporte final.
1. Filtramos `appointment_year` para mantener únicamente las consultas ocurridas entre **2021 y 2025**. *(Guardamos la tabla en el esquema `gold`)*.

### 🥇 9. Esquema Gold: Compartir Tabla a Power BI
1. Conectamos la tabla final del esquema `gold` hacia Power BI utilizando **SQL Warehouse** para crear nuestro dashboard.

### 📶 10. Power BI: Creación del Dashboard
1. **Colocar título:** "Resumen Ejecutivo de Costos Médicos".
2. **Agregar segmentación de datos:** Campo `appointment_year`, y colocarlo como lista vertical.
3. **Agregar segmentación de datos:** Campo `sex`, para filtrar por sexo.
4. **Agregar Tarjeta (KPI 1 - Costo Total):** Arrastrar el campo `appointment_cost` a valores, configurarlo como **Suma** y aplicarle formato de moneda ($).
5. **Agregar Tarjeta (KPI 2 - Total Atenciones):** Arrastrar el campo `appointmentId` a valores, configurarlo como **Recuento distintivo** para saber el volumen exacto de consultas.
6. **Agregar Tarjeta (KPI 3 - Costo Promedio):** Arrastrar el campo `appointment_cost` a valores, configurarlo como **Promedio** y aplicarle formato de moneda ($).
7. **Gráfico 1 (Barras horizontales):**
   * **Eje Y:** `insurance` (Aseguradora).
   * **Eje X:** Promedio de `appointment_cost`.
8. **Gráfico 2 (Líneas):**
   * **Eje X:** `age`
   * **Eje Y:** Promedio de `appointment_cost`.
9. **Gráfico 3 (Anillo):**
   * **Leyenda:** `appointment_cost_tier`.
   * **Valores:** Recuento de `appointmentId`.
   * *(Opcional)* Ir a formato y personalizar los colores: 🟢 low, 🟡 medium, 🟠 high, 🔴 critical.
