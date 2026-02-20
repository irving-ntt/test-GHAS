# Databricks notebook source
"""
Descripcion:
    
Subetapa: 
    26 - Cifras Control
Trámite:
    354 - IMSS Solicitud de marca de cuentas por 43 bis
Tablas input:

Tablas output:
    CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP
Tablas Delta:
    DELTA_100_MARCA_DESMARCA_{params.sr_folio}    
Archivos SQL:
    
"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso":str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_etapa":str,
    "sr_id_archivo": str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_id_snapshot":str,
    "sr_recalculo":str,
    "sr_tipo_archivo":str,
    "sr_tipo_layout":str,
})
# Validar widgets
# params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "100_MARCA_DESMARCA_INFO.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

df = db.read_data("default",statement)

DELTA_TABLE_001 = f"DELTA_100_MARCA_DESMARCA_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001,df , "overwrite")

# COMMAND ----------

if conf.debug:
    display(df.columns)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# Agregar campos nuevos
delta_800 = df.withColumn("FCN_ID_PROCESO", lit(params.sr_proceso)) \
    .withColumn("FCN_ID_SUBPROCESO", lit(params.sr_subproceso))

# Renombrar campos existentes
delta_800 = delta_800.withColumnRenamed("TOTAL_REG", "FLN_TOTAL_REGISTROS")\
        .withColumnRenamed("TOTAL_ACEPTADOS", "FLN_REG_ACEPTADOS")\
        .withColumnRenamed("TOTAL_RECHAZADOS", "FLN_REG_RECHAZADOS")\
        .withColumn("FLC_USU_REG", lit(params.sr_usuario))

#Eliminar campos que no se necesitan actualmente
delta_800 = delta_800.drop("PESOS_ACEPTADOS", "AIVS_ACEPTADOS", "PESOS_RECHAZADOS", "AIVS_RECHAZADOS")
if conf.debug:
    display(delta_800)

# COMMAND ----------

from pyspark.sql.functions import lit, col

# Agregar campos nuevos
delta_700 = df.withColumn("FCN_ID_SIEFORE", lit(81)) \
    .withColumn("FFN_ID_CONCEPTO_IMP", lit(807))

# Renombrar campos existentes
delta_700 = delta_700.withColumnRenamed("TOTAL_REG", "FTN_NUM_REG")\
        .withColumnRenamed("TOTAL_RECHAZADOS", "FTN_NUM_REG_RECH")\
        .withColumn("FTC_USU_REG", lit(params.sr_usuario))\
        .withColumnRenamed("AIVS_ACEPTADOS", "FTN_IMPORTE_ACEP_AIVS")\
        .withColumnRenamed("PESOS_ACEPTADOS", "FTN_IMPORTE_ACEP_PESOS")\
        .withColumnRenamed("PESOS_RECHAZADOS", "FTN_IMPORTE_RECH_PESOS")\
        .withColumnRenamed("AIVS_RECHAZADOS", "FTN_IMPORTE_RECH_AIVS")\
        .withColumnRenamed("FLD_FEC_REG", "FTD_FEC_REG")


#Eliminar campos que no se necesitan actualmente
delta_700 = delta_700.drop("TOTAL_ACEPTADOS")
if conf.debug:
    display(delta_700)

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.THAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

# Se elimina información por folio de la tabla histórica
delete_statement = f"DELETE FROM CIERREN.THAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# COMMAND ----------

from pyspark.sql.functions import col

# Cast columns to the correct data types
delta_800_casted = delta_800.withColumn("FTC_FOLIO", col("FTC_FOLIO").cast("string")) \
                            .withColumn("FLN_REG_ACEPTADOS", col("FLN_REG_ACEPTADOS").cast("int")) \
                            .withColumn("FLN_REG_RECHAZADOS", col("FLN_REG_RECHAZADOS").cast("int")) \
                            .withColumn("FLN_TOTAL_REGISTROS", col("FLN_TOTAL_REGISTROS").cast("int")) \
                            .withColumn("FLD_FEC_REG", col("FLD_FEC_REG").cast("timestamp")) \
                            .withColumn("FCN_ID_PROCESO", col("FCN_ID_PROCESO").cast("int")) \
                            .withColumn("FCN_ID_SUBPROCESO", col("FCN_ID_SUBPROCESO").cast("int")) \
                            .withColumn("FLC_USU_REG", col("FLC_USU_REG").cast("string"))

if delta_800_casted.count() > 0:
    table_800 = "CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP"
    db.write_data(delta_800_casted, f"{table_800}", "default", "append")
    # Inserción en tabla historica
    table_002 = "THAFOGRAL_VAL_CIFRAS_CTRL_RESP"
    db.write_data(delta_800_casted, f"{table_002}", "default", "append")



# COMMAND ----------

if delta_700.count() > 0:
    # Escribir solo los nuevos registros en la tabla
    #display(delta_700.columns)
    table_700 = "CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS"
    db.write_data(delta_700, f"{table_700}", "default", "append")

    # Inserción en tabla historica
    table_003 = "CIERREN.THAFOTRAS_SUM_ARCHIVO_TRANS"
    db.write_data(delta_700, f"{table_003}", "default", "append")
