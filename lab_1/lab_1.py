# Databricks notebook source
# MAGIC %md
# MAGIC 📜 1. Librerías
# MAGIC
# MAGIC Importamos las funciones de PySpark para transformar datos.
# MAGIC * `from pyspark.sql.functions import *`
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC  2. Arquitectura:
# MAGIC  
# MAGIC  Creación de Catálogo, Esquemas y Volúmenes
# MAGIC 1. Crear el catálogo `Lab_1`.
# MAGIC 2. Crear los esquemas `bronze`, `silver` y `gold` dentro del catálogo.
# MAGIC 3. Crear el volumen `bronze_vol` dentro del esquema `bronze`.
# MAGIC 4. Crear la carpeta `files` dentro del volumen.
# MAGIC 5. Cargar manualmente los archivos `patients.csv`, `location.csv` y `appointments.txt` en la carpeta.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- empezar de cero
# MAGIC -- drop catalog if exists lab_1 cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists lab_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists lab_1.bronze;
# MAGIC create schema if not exists lab_1.silver;
# MAGIC create schema if not exists lab_1.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists lab_1.bronze.bronze_vol;

# COMMAND ----------

import os
os.makedirs("/Volumes/lab_1/bronze/bronze_vol/files", exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 🥉 3. Esquema Bronze: Creación de Tablas
# MAGIC
# MAGIC
# MAGIC Tomamos los datos de los archivos y los guardamos como tablas en el esquema `bronze`.
# MAGIC 1. Crear la tabla `patients` usando los datos de `patients.csv` *(Opción 1: Usando SQL)*.
# MAGIC 2. Crear la tabla `patients` usando los datos de `patients.csv` *(Opción 2: Usando PySpark)*.
# MAGIC 3. Crear la tabla `location` usando los datos de `location.csv` *(Usando PySpark)*.
# MAGIC 4. Crear la tabla `appointments` usando los datos de `appointments.csv` *(Usando PySpark)*.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table lab_1.bronze.patients as (
# MAGIC   select * from read_files(
# MAGIC     '/Volumes/lab_1/bronze/bronze_vol/files/patients.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     inferschema => false,
# MAGIC     delimiter => ';'
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC select * from lab_1.bronze.patients limit 100

# COMMAND ----------

df_patients = spark\
    .read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "false")\
    .option("delimiter", ";")\
    .load("/Volumes/lab_1/bronze/bronze_vol/files/patients.csv")

df_patients\
.write\
.mode("overwrite")\
.saveAsTable("lab_1.bronze.patients")

display(df_patients.limit(100))

# COMMAND ----------

df_location = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "false")\
    .option("delimiter", ",")\
    .load("/Volumes/lab_1/bronze/bronze_vol/files/location.csv")

df_location\
    .write\
    .mode("overwrite")\
    .saveAsTable("lab_1.bronze.location")

display(df_location.limit(100))

# COMMAND ----------

df_appointments = spark\
    .read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema", "false")\
    .option("delimiter", "\t")\
    .load("/Volumes/lab_1/bronze/bronze_vol/files/appointments.txt")

df_appointments\
    .write\
    .mode("overwrite")\
    .saveAsTable("lab_1.bronze.appointments")

display(df_appointments.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC 🥈 4. Esquema Silver: Transformación de Tipos de Datos y Nombres
# MAGIC
# MAGIC
# MAGIC Tomamos las tablas del esquema `bronze`, aseguramos que los tipos de datos sean correctos, igualamos los nombres de las columnas de cruce y las guardamos en el esquema `silver`.
# MAGIC
# MAGIC 1. **Tabla `patients`:**
# MAGIC    * `birthdate` (pasa a tipo Fecha) y `locationId` (pasa a tipo Entero).
# MAGIC    * `Id` (pasa a llamarse `patientId`).
# MAGIC    * Guardamos la tabla en el esquema `silver`.
# MAGIC 2. **Tabla `appointments`:**
# MAGIC    * `appointment_date` (pasa a tipo Fecha) y `appointment_cost` (pasa a tipo Decimal).
# MAGIC    * `Id` (pasa a llamarse `appointmentId`).
# MAGIC    * Guardamos la tabla en el esquema `silver`.
# MAGIC 3. **Tabla `location`:**
# MAGIC    * `Id` (pasa a tipo Entero).
# MAGIC    * `Id` (pasa a llamarse `locationId`).
# MAGIC    * Guardamos la tabla en el esquema `silver`.
# MAGIC

# COMMAND ----------

df_patients_silver = spark.read.table("lab_1.bronze.patients")\
    .withColumn("birthdate", to_date(col("birthdate"), "yyyy-MM-dd"))\
    .withColumn("locationId", col("locationId").cast("integer"))\
    .withColumnRenamed("Id", "patientId")

df_patients_silver.write\
    .mode("overwrite")\
    .saveAsTable("lab_1.silver.patients")

display(df_patients_silver.limit(100))

# COMMAND ----------

df_appointments_silver = spark.read.table("lab_1.bronze.appointments")\
    .withColumn("appointment_date", to_date("appointment_date", "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
    .withColumn("appointment_cost", col("appointment_cost").cast("double"))\
    .withColumnRenamed("Id", "appointmentId")

df_appointments_silver.write\
    .mode("overwrite")\
    .saveAsTable("lab_1.silver.appointments")

display(df_appointments_silver.limit(100))

# COMMAND ----------

df_location_silver = spark.read.table("lab_1.bronze.location")\
    .withColumn("Id", col("Id").cast("integer"))\
    .withColumnRenamed("Id", "locationId")

df_location_silver.write\
    .mode("overwrite")\
    .saveAsTable("lab_1.silver.location")

display(df_location_silver.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC 🥈 5. Esquema Silver: Cruce de Tablas (Join)
# MAGIC
# MAGIC
# MAGIC 1. Unimos pacientes (`patients`) con sus consultas médicas (`appointments`) y su ubicación (`location`). 
# MAGIC *(Nota: Seleccionamos los campos en común una sola vez para evitar columnas duplicadas).*
# MAGIC

# COMMAND ----------

df_join = spark.read.table("lab_1.silver.patients").alias("p")\
    .join(spark.read.table("lab_1.silver.appointments").alias("a"), col("p.patientId") == col("a.patientId"), "left")\
    .join(spark.read.table("lab_1.silver.location").alias("l"), col("p.locationId") == col("l.locationId"), "left")\
    .select("p.patientId", "p.birthdate", "p.SSN", "p.first_name", "p.last_name", "p.sex",
            "a.appointmentId", "a.appointment_date", "a.insurance", "a.appointment_cost",
            "l.locationId", "l.city", "l.state", "l.county"
            )

display(df_join.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 🥈 6. Esquema Silver: Agregar Nuevos Campos
# MAGIC Agregamos nuevos campos a la tabla cruzada:
# MAGIC * **`age`**: Calculamos la edad actual del paciente basándonos en su fecha de nacimiento (`birthdate`) y la fecha actual.
# MAGIC * **`appointment_year`**: Extraemos el año de la fecha de consulta (`appointment_date`).
# MAGIC * **`appointment_cost_tier`**: Categorizamos el costo de la consulta médica (`appointment_cost`) en:
# MAGIC   * 🟢 **low:** menor a $500
# MAGIC   * 🟡 **medium:** entre $500 y $3000
# MAGIC   * 🟠 **high:** entre $3000 y $20000
# MAGIC   * 🔴 **critical:** mayor a $20000
# MAGIC

# COMMAND ----------

df_age = df_join\
    .withColumn("age", floor(months_between(current_date(), col("birthdate"))/12).cast("integer"))

display(df_age.limit(100))

# COMMAND ----------

df_year = df_age\
    .withColumn("year", date_format("appointment_date", "yyyy").cast("integer"))

display(df_year.limit(100))

# COMMAND ----------

df_tier = df_year\
    .withColumn("appointment_cost_tier",
                when(col("appointment_cost") <= 500, "low")
                .when(col("appointment_cost") <= 3000, "medium")
                .when(col("appointment_cost") <= 20000, "high")
                .otherwise("critical")
                )

display(df_tier.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC 🥈 7. Esquema Silver: Limpieza y Calidad de Datos
# MAGIC 1. **Filtrado de errores:** Eliminamos filas que cumplan alguna de estas condiciones:
# MAGIC    * `patientId` nulo.
# MAGIC    * `appointmentId` nulo.
# MAGIC    * Identificación `SSN` nula.
# MAGIC    * Edad (`age`) negativa.
# MAGIC    * Costo de consulta (`appointment_cost`) negativo.
# MAGIC 2. **Eliminamos consultas médicas duplicadas** (filas donde `patientId`, `appointmentId` y `appointment_date` se repitan al mismo tiempo).
# MAGIC 3. Guardamos la tabla limpia como `consolidated` en el esquema `silver`.
# MAGIC 4. Guardamos los datos descartados en una tabla llamada `rejected` para futuras auditorías.
# MAGIC

# COMMAND ----------

df_limpio = df_tier\
    .filter(~(col("patientId").isNull() | col("appointmentId").isNull() | col("SSN").isNull() | (col("age") < 0) | (col("appointment_cost") < 0)))\
    .dropDuplicates(["patientId", "appointmentId", "appointment_date"])

df_limpio.write\
    .mode("overwrite")\
    .saveAsTable("lab_1.silver.consolidated")

df_rejectado = df_tier\
    .filter((col("patientId").isNull() | col("appointmentId").isNull() | col("SSN").isNull() | (col("age") < 0) | (col("appointment_cost") < 0)))

df_rejectado.write\
    .mode("overwrite")\
    .saveAsTable("lab_1.silver.rejected")


display(df_limpio.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC 🥇 8. Esquema Gold: Filtrar los datos
# MAGIC
# MAGIC
# MAGIC Tomamos la tabla `consolidated` del esquema `silver` y filtramos solo lo necesario para nuestro reporte final.
# MAGIC

# COMMAND ----------

df_consolidated_2021_2025 = spark.read.table("lab_1.silver.consolidated")\
    .filter(col("year").isin(["2021", "2022", "2023", "2024", "2025"]))

df_consolidated_2021_2025.write\
.mode("overwrite")\
.saveAsTable("lab_1.gold.consolidated_2021_2025")

display(df_consolidated_2021_2025.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC 🥇 9. Esquema Gold: Compartir Tabla a Power BI
# MAGIC 1. Conectamos la tabla final del esquema `gold` hacia Power BI utilizando **SQL Warehouse** para crear nuestro dashboard.
# MAGIC
# MAGIC