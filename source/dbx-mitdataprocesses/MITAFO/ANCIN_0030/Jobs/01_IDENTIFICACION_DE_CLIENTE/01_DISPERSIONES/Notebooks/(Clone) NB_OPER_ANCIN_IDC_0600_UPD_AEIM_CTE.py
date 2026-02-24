# Databricks notebook source
# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_origen_arc": str,
    "sr_dt_org_arc": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
    "var_tramite": str,
})
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0600_UPD_AEIM")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Leemos TEMP_IDENTIFICACIONCLIENTES_07_<sr_id_archivo>

# COMMAND ----------

DF_7OO_CARGA = db.read_delta(f"TEMP_IDENTIFICACIONCLIENTES_07_{params.sr_id_archivo}")

# COMMAND ----------

DF_7OO_CARGA = DF_7OO_CARGA.select(
    col("FTD_FECHA_CERTIFICACION").cast("timestamp").alias("FTD_FECHA_CERTIFICACION"),
    coalesce(col("FTN_NSS_CURP"), lit(0)).alias("FTC_CLAVE_TMP"),
    col("FTC_IDENTIFICADOS").cast("decimal(1,0)").alias("FTN_ESTATUS_DIAG"),
    lit(0).cast("int").alias("FNC_REG_PATRONAL_IMSS"),
    when(col("FTC_IDENTIFICADOS") == "1", lit(560))
    .otherwise(lit(559))
    .cast("int")
    .alias("FTN_ESTATUS_REG"),
    when(col("FTC_IDENTIFICADOS") == "1", lit(821))
    .otherwise(lit(562))
    .cast("int")
    .alias("FTN_MOTIVO"),
)
DF_7OO_CARGA = DF_7OO_CARGA.select(
    "FTN_NSS_CURP",
    "FTN_ESTATUS_DIAG",
    "FTN_NUM_CTA_INVDUAL",
    "FTC_ID_DIAGNOSTICO",
    "FTN_ID_SUBP_NO_CONV",
    "FTD_FECHA_CERTIFICACION",
    "FTN_CTE_PENSIONADO",
    "FTC_FOLIO",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTN_ID_ARCHIVO",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_CLAVE_ENT_RECEP",
    "FNC_REG_PATRONAL_IMSS",
    "FTN_NSS",
    "FTN_ESTATUS_REG",
    "FTN_MOTIVO",
    "FCN_ID_TIPO_SUBCTA"
)
if conf.debug:
    display(DF_7OO_CARGA)