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

DELTA_TABLE_DISPERSION = "DELTA_ACLARACIONES_ESPECIALES_DISPERSION_" + params.sr_id_archivo
DELTA_SALIDA_001 = "DELTA_ACLARACIONES_ESPECIALES_SALIDA_47_DISPERSION_" + params.sr_id_archivo

DELTA_TABLE_47 = "DELTA_INCO_INT_TOT_" + params.sr_folio

# COMMAND ----------

#Leer desde OCI
statement_001 = query.get_statement(
    "ACLARACIONES_ESPECIALES_ISSSTE_002.sql",
    SR_FOLIO=params.sr_folio,
)
df_dispersion = db.read_data("default", statement_001)

#Escribe delta
db.write_delta(DELTA_TABLE_DISPERSION, df_dispersion, "overwrite")

# COMMAND ----------

# DBTITLE 1,Tabla Resultante de la consulta
if conf.debug:
    display(df_dispersion)

# COMMAND ----------

columns_selected = ["FCN_ID_TIPO_SUBCTA", "FTC_FOLIO","FTN_ID_ARCHIVO","FTN_NO_LINEA","FNN_ID_REFERENCIA","FCN_ID_PROCESO",
                    "FCN_ID_SUBPROCESO","FTF_MONTO_PESOS","FTF_MONTO_ACCIONES","FTC_TABLA_NCI_MOV","FFN_ID_CONCEPTO_MOV","FTN_NSS",
                    "FTC_CURP","FTN_ID_MONTO","FNN_ID_VIV_GARANTIA","FTN_TIPO_REG"]

# COMMAND ----------

from pyspark.sql.functions import lit, col

if spark.catalog.tableExists(DELTA_TABLE_47):
    df_47 = db.read_delta(DELTA_TABLE_47).select(columns_selected)
    if df_47.count() == 0:
        df = df_dispersion
    else:
        schema_dispersion = df_dispersion.schema
        columns = [
            col(field.name).cast(field.dataType).alias(field.name) 
            for field in schema_dispersion 
            if field.name in df_47.columns
        ]
        df_47_ext = df_47.select(*columns)
        df_47_ext = df_47_ext.withColumn("FCN_ID_SIEFORE", lit(0)).withColumn("FTN_NUM_CTA_INVDUAL", lit(0))
        df = df_47_ext.unionByName(df_dispersion, allowMissingColumns=True)
else:
    df = df_dispersion

# COMMAND ----------

# DBTITLE 1,Union tabla dispersion, df_47
if conf.debug:
    display(df)

# COMMAND ----------

from pyspark.sql.functions import col, substring

df = df.withColumn("FNN_ID_REFERENCIA", substring(col("FNN_ID_REFERENCIA"), 1, 8))
df = df.withColumn("FTN_TIPO_REG", substring(col("FTN_TIPO_REG"), 1, 1))
db.write_delta(DELTA_SALIDA_001, df, "overwrite")

# COMMAND ----------

# DBTITLE 1,Tabla resultante
if conf.debug:
    display(df)
