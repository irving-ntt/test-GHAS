# Databricks notebook source
'''
Descripcion:
    Generación de tabla delta 200 IMPORTE SIEFORE
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    120 - DOISSSTE
    122 - DITISSSTE
Tablas INPUT:
    CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE
    CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_100_#SR_ID_ARCHIVO#
Archivos SQL:
    ACLARACIONES_ESPECIALES_ISSSTE_001.sql
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
    "sr_folio" : str,
    "sr_id_archivo": str,
    "sr_fec_arc" : str,
    "sr_fec_liq" : str,
    "sr_dt_org_arc" : str,
    "sr_origen_arc" : str,
    "sr_tipo_layout": str,
    "sr_tipo_reporte" : str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_etapa": str,
    "sr_id_snapshot": str
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

from pyspark.sql.functions import coalesce, lit

# COMMAND ----------

DELTA_SALIDA_001 = "DELTA_ACLARACIONES_ESPECIALES_SALIDA_47_DISPERSION_" + params.sr_id_archivo
DELTA_TABLE_100 = "DELTA_ACLARACIONES_ESPECIALES_100_" + params.sr_id_archivo

DELTA_SALIDA_002 = "DELTA_ACLARACIONES_ESPECIALES_100_SALIDA_" + params.sr_id_archivo

# COMMAND ----------

df_100 = db.read_delta(DELTA_TABLE_100)
display(df_100)

# COMMAND ----------

df_salida = db.read_delta(DELTA_SALIDA_001)

# COMMAND ----------

df_100 = db.read_delta(DELTA_TABLE_100)
df_salida = db.read_delta(DELTA_SALIDA_001)

# Renombrar columna FTN_NUM_CTA_INVDUAL para evitar duplicidad de columnas
df_100 = db.read_delta(DELTA_TABLE_100).withColumnRenamed("FTN_NUM_CTA_INVDUAL", "FTN_NUM_CTA_INVDUAL_100")
df_salida = db.read_delta(DELTA_SALIDA_001).withColumnRenamed("FTN_NUM_CTA_INVDUAL", "FTN_NUM_CTA_INVDUAL_salida")

# Join entre df de salida y el generado en el notebook carga inicial issste
df = df_salida.join(df_100, ["FTC_CURP", "FTC_FOLIO", "FTN_NSS"], "inner")

# Unificación de la columna FTN_NUM_CTA_INVDUAL
df = df.withColumn(
    "FTN_NUM_CTA_INVDUAL",
    coalesce(df["FTN_NUM_CTA_INVDUAL_salida"], df["FTN_NUM_CTA_INVDUAL_100"])
)

# COMMAND ----------

df = df.withColumn("FCN_ID_SIEFORE", coalesce(df["FCN_ID_SIEFORE"], lit(846))) \
       .withColumn("FTN_ESTATUS", coalesce(df["FTN_ESTATUS"], lit(0))) \
       .withColumnRenamed("FNN_ID_VIV_GARANTIA", "FTN_ID_CRED_VIV") \
       .drop("FNN_ID_REFERENCIA", "FCN_ID_PROCESO", "FCN_ID_SUBPROCESO", "FTN_NSS", "FTC_CURP","FFN_ID_CONCEPTO_MOV","FTN_NUM_CTA_INVDUAL_salida", "FTN_NUM_CTA_INVDUAL_100")

# COMMAND ----------

db.write_delta(DELTA_SALIDA_002, df, "overwrite")

# COMMAND ----------

# DBTITLE 1,Tabla resultante join df_47, df_inicial
if conf.debug:
    display(df)
