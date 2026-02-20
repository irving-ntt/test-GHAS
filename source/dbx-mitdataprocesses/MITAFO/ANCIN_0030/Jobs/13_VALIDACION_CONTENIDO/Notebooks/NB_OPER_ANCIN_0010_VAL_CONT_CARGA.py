# Databricks notebook source
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
    "sr_folio" :str,
    "sr_instancia_proceso" :str,
    "sr_usuario" :str,
    "sr_etapa" :str,
    "sr_id_snapshot" :str,
    "sr_paso" :str,
    "sr_id_archivo_siguiente": str,
    "sr_fec_arc": str
})
# Validar widgets
#params.validate()

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

#Carga archivo y crea la delta
df = load_file(SETTINGS.GENERAL.EXTERNAL_LOCATION, params.sr_path_arch)

DELTA_TABLE_001 = "DELTA_VALIDA_CONTENIDO_" + (params.sr_id_archivo if params.sr_id_archivo != "1" else params.sr_folio)

db.drop_delta(DELTA_TABLE_001)

#Escribe delta
db.write_delta(DELTA_TABLE_001, df, "overwrite")

# COMMAND ----------

#import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, LongType, IntegerType,StructType, StructField

schema = StructType([
	StructField("FTC_FOLIO", StringType(), True),
	StructField("FTC_ID_SUBETAPA", IntegerType(), True),
	StructField("FLN_TOTAL_REGISTROS", LongType(), True),
	StructField("FLN_REG_CUMPLIERON", LongType(), True),
	StructField("FLN_REG_NO_CUMPLIERON", LongType(), True),
	StructField("FLC_VALIDACION", StringType(), True),
	StructField("FLN_TOTAL_ERRORES", LongType(), True),
	StructField("FLC_DETALLE", StringType(), True),
	StructField("FLD_FEC_REG", StringType(), True),
	StructField("FLC_USU_REG", StringType(), True),
	StructField("FTC_FOLIO_REL", StringType(), True),
	StructField("FTN_ID_ARCHIVO", IntegerType(), True)
	])

df = spark.createDataFrame([], schema)

DELTA_TABLE_002 = "DELTA_VALIDA_CONTENIDO_CIFRAS_CONTROL_" + (params.sr_id_archivo if params.sr_id_archivo != "1" else params.sr_folio)

db.drop_delta(DELTA_TABLE_002)

db.write_delta(DELTA_TABLE_002, df, "overwrite")

# COMMAND ----------


dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)

