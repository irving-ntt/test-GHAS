# Databricks notebook source
"""
Descripcion:
    Extracción de información de Solicitud de Desmarca para obtencion de cifras control
Subetapa: 
    26 - Cifras Control
Trámite:
    3283 - Desmarca FOVISSSTE
Tablas input:
    PROCESOS.TTAFOTRAS_DESMARCA_FOVST
Tablas output:
    CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP
    CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS
Tablas Delta:
    N/A
Archivos SQL:
    100_DEV_EXC_INF.sql
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

statement_001 = query.get_statement(
    "100_DEV_EXC_INF.sql",
    SR_FOLIO=params.sr_folio,
    SR_PROCESO=params.sr_proceso,
    SR_SUBPROCESO=params.sr_subproceso,
    USUARIO=params.sr_usuario,
)

df = db.read_data("default",statement_001)
if conf.debug:
    display(df)

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.TLAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# Se elimina información por folio de la tabla histórica
delete_statement_01 = f"DELETE FROM CIERREN.THAFOGRAL_VAL_CIFRAS_CTRL_RESP WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement_01, async_mode=False
    )

# COMMAND ----------

delete_statement = f"DELETE FROM CIERREN.TTAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement, async_mode=False
    )

# Se elimina información por folio de la tabla histórica
delete_statement_01 = f"DELETE FROM CIERREN.THAFOTRAS_SUM_ARCHIVO_TRANS WHERE FTC_FOLIO='{params.sr_folio}'"
execution = db.execute_oci_dml(
        statement=delete_statement_01, async_mode=False
    )

# COMMAND ----------

table_001 = "TLAFOGRAL_VAL_CIFRAS_CTRL_RESP"
db800 = df.select(["FTC_FOLIO", "FCN_ID_PROCESO", "FCN_ID_SUBPROCESO", "FLN_TOTAL_REGISTROS", "FLN_REG_ACEPTADOS", "FLN_REG_RECHAZADOS", "FLD_FEC_REG", "FLC_USU_REG"])
if conf.debug:
    display(db800)
db.write_data(db800, f"{table_001}", "default", "append")

# COMMAND ----------

# Se inserta información en tabla de histórica
table_002 = "THAFOGRAL_VAL_CIFRAS_CTRL_RESP"
db.write_data(db800, f"{table_002}", "default", "append")

# COMMAND ----------

table_003 = "TTAFOTRAS_SUM_ARCHIVO_TRANS"
db600 = df.select(["FTC_FOLIO","FCN_ID_SIEFORE","FFN_ID_CONCEPTO_IMP","FTN_NUM_REG","FTN_NUM_REG_RECH","FTN_IMPORTE_ACEP_AIVS","FTN_IMPORTE_ACEP_PESOS","FTN_IMPORTE_RECH_AIVS","FTN_IMPORTE_RECH_PESOS","FTD_FEC_REG","FTC_USU_REG"])
if conf.debug:
    display(db600)
db.write_data(db600, f"{table_003}", "default", "append")

# COMMAND ----------

# Se inserta información en tabla de histórica
table_004 = "THAFOTRAS_SUM_ARCHIVO_TRANS"
db.write_data(db600, f"{table_004}", "default", "append")
