# Databricks notebook source
"""
Descripcion:
    Identifica las tabla delta del proceso y las elimina
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    COMUN -
Tablas output:
    N/A 
Tablas Delta:
    N/A 
Archivos SQL:
    N/A 


    
    """

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

conf = ConfManager()


# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_tipo_mov": str,
    "sr_conv_ingty": str,
    "sr_path_arch": str,
    "sr_tipo_ejecucion": str,
    # Agregamos valores faltantes para notificacion
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

# Notify.send_notification("INFO", params)


# COMMAND ----------

from pyspark.sql.functions import col

Tablas_Persistentes = []

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("temp_delta_comun_mcv")) \
    .filter(col("tableName").endswith(params.sr_folio)) \
    .filter(~col("tableName").isin(Tablas_Persistentes))

display(tables)

# COMMAND ----------

# DBTITLE 1,Elimina Tablas DELTA
from pyspark.sql.functions import col

Tablas_Persistentes = []

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("temp_delta_comun_mcv")) \
    .filter(col("tableName").endswith(params.sr_folio)) \
    .filter(~col("tableName").isin(Tablas_Persistentes))

display(tables)

# Drop the tables with the specified folio
for row in tables.collect():
    table_name = row.tableName
    try: 
        spark.sql(f"DROP TABLE IF EXISTS {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{table_name}")
        print(f"tabla {table_name} borrada")
    except:
        print(f"Error al borrar: {table_name} - Permisos insuficientes") 


# Forzar la recolección de basura
import gc
gc.collect()

print("Proceso de eliminación de tablas y liberación de memoria completado.")

# COMMAND ----------

# MAGIC %md
# MAGIC Las celdas siguientes no tienen funcion  ya que en la celda anterior fueron eliminadas las tablas delta identificadas

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row

Tablas_Persistentes = []

# 1. Listar tablas filtradas
tables = spark.sql(f"SHOW TABLES IN {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}") \
    .filter(col("tableName").startswith("temp_delta_comun_mcv")) \
    .filter(col("tableName").endswith(params.sr_folio)) \
    .filter(~col("tableName").isin(Tablas_Persistentes))

# 2. Obtener la lista de nombres de tabla
table_names = [row['tableName'] for row in tables.collect()]

# 3. Para cada tabla, obtener detalles con DESCRIBE DETAIL
details = []
for table_name in table_names:
    full_table_name = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{table_name}"
    detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
    row = detail_df.select("name", "createdAt", "lastModified").first()
    # Puedes agregar más columnas si lo deseas, como 'location', 'sizeInBytes', etc.
    details.append(Row(tableName=row['name'], createdAt=row['createdAt'], lastModified=row['lastModified']))

# 4. Crear un DataFrame con los detalles
if details:
    details_df = spark.createDataFrame(details)
    display(details_df)
else:
    print("No hay tablas que cumplan los criterios.")


# COMMAND ----------

# DBTITLE 1,Eliminacion 2a version
from datetime import datetime
from pyspark.sql import Row

eliminacion_log = []

for row in details_df.collect():
    table_name = row['tableName']
    full_table_name = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{table_name}"
    fecha_eliminacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        exito = True
        print(f"Eliminada exitosamente: {full_table_name}")
    except Exception as e:
        exito = False
        print(f"Error al eliminar {full_table_name}: {e}")
    eliminacion_log.append(Row(
        tableName=table_name,
        fechaEliminacion=fecha_eliminacion,
        exito=exito
    ))

# Crear un DataFrame con el registro de eliminación
if eliminacion_log:
    eliminacion_df = spark.createDataFrame(eliminacion_log)
    display(eliminacion_df)
else:
    print("No se intentó eliminar ninguna tabla.")

