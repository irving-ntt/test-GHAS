# Databricks notebook source
# DBTITLE 1,DOCSTRING
'''
Descripcion:
    Realiza la carga del archivo hacia OCI
Subetapa:
    20 - CARGA DE ARCHIVO
Trámite:
    8 - DOIMSS
    120 - DOISSSTE
Tablas INPUT:
    N/A
Tablas OUTPUT:
    CIERREN_ETL.TTSISGRAL_ETL_LEE_ARCHIVO
Tablas INPUT DELTA:
    DELTA_CARGA_ARCHIVO_001_#SR_ID_ARCHIVO#
Tablas OUTPUT DELTA:
    DELTA_CARGA_ARCHIVO_001_#SR_ID_ARCHIVO#
    DELTA_CARGA_ARCHIVO_002_#SR_ID_ARCHIVO#
Archivos SQL:
    CARGA_ARCHIVO_001.sql
    CARGA_ARCHIVO_002.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_id_archivo": str,
    "sr_path_arch": str,
    "sr_origen_arc": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

def load_file(storage_location, sr_path_arch):
    df = spark.read.text(storage_location + sr_path_arch)
    df = df.withColumnRenamed("value", "FTC_LINEA")
    indexed_df = df.withColumn("idx", monotonically_increasing_id() + 1)
    w = Window.orderBy("idx")
    indexed_df = indexed_df.withColumn("FTN_NO_LINEA", row_number().over(w))

    indexed_df = indexed_df.withColumnRenamed("_c0", "FTC_LINEA")
    indexed_df = indexed_df.drop("idx")
    return indexed_df

# COMMAND ----------

df = load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION, params.sr_path_arch)

DELTA_TABLE_001 = "DELTA_CARGA_ARCHIVO_001_" + params.sr_id_archivo

#Escribe delta
db.write_delta(DELTA_TABLE_001, df, "overwrite")

#Genera la consulta y le pasa los parametros
statement_001 = query.get_statement(
    "CARGA_ARCHIVO_001.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_001}",
    FTN_ID_ARCHIVO=params.sr_id_archivo
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

db.write_data(df, f"{conf.conn_schema}.{conf.table_001}" , "default", "append")

# COMMAND ----------

db.drop_delta(DELTA_TABLE_001)

# COMMAND ----------

Notify.send_notification("CARG_ARCH", params)