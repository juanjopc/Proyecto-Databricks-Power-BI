En este proyecto realizaremos un análisis completo de los **costos de consultas médicas** de pacientes. Aprenderemos a construir un flujo de datos (ETL) desde cero utilizando **Databricks** con la arquitectura Medallón (Bronze, Silver, Gold) y PySpark para limpiar y transformar los datos. Finalmente, conectaremos nuestros datos ya procesados a **Power BI** para crear un dashboard interactivo que nos permitirá analizar los costos por edad, género y aseguradora.

### 📜 1. Librerías
[[05:15]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=5m15s) Importamos las funciones de PySpark para transformar datos.
* [[05:21]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=5m21s) `from pyspark.sql.functions import *`

### 🏛️ 2. Arquitectura: Creación de Catálogo, Esquemas y Volúmenes
1. [[10:47]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=10m47s) Crear el catálogo `lab_1`.
2. [[12:06]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=12m06s) Crear los esquemas `bronze`, `silver` y `gold` dentro del catálogo.
3. [[14:42]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=14m42s) Crear el volumen `bronze_vol` dentro del esquema `bronze`.
4. [[15:40]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=15m40s) Crear la carpeta `files` dentro del volumen.
5. [[17:41]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=17m41s) Cargar manualmente los archivos `patients.csv`, `location.csv` y `appointments.txt` en la carpeta.

### 🥉 3. Esquema Bronze: Creación de Tablas
[[19:10]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=19m10s) Tomamos los datos de los archivos y los guardamos como tablas en el esquema `bronze`.
1. [[19:56]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=19m56s) Crear la tabla `patients` usando los datos de `patients.csv` *(Opción 1: Usando SQL)*.
2. [[24:42]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=24m42s) Crear la tabla `patients` usando los datos de `patients.csv` *(Opción 2: Usando PySpark)*.
3. [[30:42]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=30m42s) Crear la tabla `location` usando los datos de `location.csv` *(Usando PySpark)*.
4. [[28:45]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=28m45s) Crear la tabla `appointments` usando los datos de `appointments.csv` *(Usando PySpark)*.

### 🥈 4. Esquema Silver: Transformación de Tipos de Datos y Nombres
[[32:37]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=32m37s) Tomamos las tablas del esquema `bronze`, aseguramos que los tipos de datos sean correctos, igualamos los nombres de las columnas de cruce y las guardamos en el esquema `silver`.

1. [[34:04]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=34m04s) **Tabla `patients`:**
   * `birthdate` (pasa a tipo Fecha) y `locationId` (pasa a tipo Entero).
   * `Id` (pasa a llamarse `patientId`).
   * Guardamos la tabla en el esquema `silver`.
2. [[40:10]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=40m10s) **Tabla `appointments`:**
   * `appointment_date` (pasa a tipo Fecha) y `appointment_cost` (pasa a tipo Decimal).
   * `Id` (pasa a llamarse `appointmentId`).
   * Guardamos la tabla en el esquema `silver`.
3. [[45:28]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=45m28s) **Tabla `location`:**
   * `Id` (pasa a tipo Entero).
   * `Id` (pasa a llamarse `locationId`).
   * Guardamos la tabla en el esquema `silver`.

### 🥈 5. Esquema Silver: Cruce de Tablas (Join)
1. [[47:13]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=47m13s) Unimos pacientes (`patients`) con sus consultas médicas (`appointments`) y su ubicación (`location`). 
*(Nota: Seleccionamos los campos en común una sola vez para evitar columnas duplicadas).*

### 🥈 6. Esquema Silver: Agregar Nuevos Campos
[[53:50]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=53m50s) Agregamos nuevos campos a la tabla cruzada:
* [[54:49]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=54m49s) **`age`**: Calculamos la edad actual del paciente basándonos en su fecha de nacimiento (`birthdate`) y la fecha actual.
* [[58:19]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=58m19s) **`appointment_year`**: Extraemos el año de la fecha de consulta (`appointment_date`).
* [[59:47]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=59m47s) **`appointment_cost_tier`**: Categorizamos el costo de la consulta médica (`appointment_cost`) en:
  * 🟢 **low:** menor a $500
  * 🟡 **medium:** entre $500 y $3000
  * 🟠 **high:** entre $3000 y $20000
  * 🔴 **critical:** mayor a $20000

### 🥈 7. Esquema Silver: Limpieza y Calidad de Datos
1. [[1:04:06]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h4m6s) **Filtrado de errores:** Eliminamos filas que cumplan alguna de estas condiciones:
   * `patientId` nulo.
   * `appointmentId` nulo.
   * Identificación `SSN` nula.
   * Edad (`age`) negativa.
   * Costo de consulta (`appointment_cost`) negativo.
2. [[1:07:02]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h7m2s) **Eliminamos consultas médicas duplicadas** (filas donde `patientId`, `appointmentId` y `appointment_date` se repitan al mismo tiempo).
3. [[1:07:50]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h7m50s) Guardamos la tabla limpia como `consolidated` en el esquema `silver`.
4. [[1:08:33]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h8m33s) Guardamos los datos descartados en una tabla llamada `rejected` para futuras auditorías.

### 🥇 8. Esquema Gold: Filtrar los datos
[[1:10:51]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h10m51s) Tomamos la tabla `consolidated` del esquema `silver` y filtramos solo lo necesario para nuestro reporte final.
1. [[1:11:10]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h11m10s) Filtramos `appointment_year` para mantener únicamente las consultas ocurridas entre **2021 y 2025**. *(Guardamos la tabla en el esquema `gold`)*.

### 🥇 9. Esquema Gold: Compartir Tabla a Power BI
1. [[1:14:20]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h14m20s) Conectamos la tabla final del esquema `gold` hacia Power BI utilizando **SQL Warehouse** para crear nuestro dashboard.

### 📶 10. Power BI: Creación del Dashboard
1. [[1:18:53]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h18m53s) **Colocar título:** "Resumen Ejecutivo de Costos Médicos".
2. [[1:19:35]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h19m35s) **Agregar segmentación de datos:** Campo `appointment_year`, y colocarlo como lista vertical.
3. [[1:20:30]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h20m30s) **Agregar segmentación de datos:** Campo `sex`, para filtrar por sexo.
4. [[1:21:06]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h21m6s) **Agregar Tarjeta (KPI 1 - Costo Total):** Arrastrar el campo `appointment_cost` a valores, configurarlo como **Suma** y aplicarle formato de moneda ($).
5. [[1:22:17]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h22m17s) **Agregar Tarjeta (KPI 2 - Total Atenciones):** Arrastrar el campo `appointmentId` a valores, configurarlo como **Recuento distintivo** para saber el volumen exacto de consultas.
6. [[1:22:51]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h22m51s) **Agregar Tarjeta (KPI 3 - Costo Promedio):** Arrastrar el campo `appointment_cost` a valores, configurarlo como **Promedio** y aplicarle formato de moneda ($).
7. [[1:23:46]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h23m46s) **Gráfico 1 (Barras horizontales):**
   * **Eje Y:** `insurance` (Aseguradora).
   * **Eje X:** Promedio de `appointment_cost`.
8. [[1:25:00]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h25m0s) **Gráfico 2 (Líneas):**
   * **Eje X:** `age`
   * **Eje Y:** Promedio de `appointment_cost`.
9. [[1:25:50]](https://www.youtube.com/watch?v=Q_a4ZoAWY9M&t=1h25m50s) **Gráfico 3 (Anillo):**
   * **Leyenda:** `appointment_cost_tier`.
   * **Valores:** Recuento de `appointmentId`.
   * *(Opcional)* Ir a formato y personalizar los colores: 🟢 low, 🟡 medium, 🟠 high, 🔴 critical.

