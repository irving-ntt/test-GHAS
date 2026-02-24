# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

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
display(query.get_sql_list())

# COMMAND ----------

statement_001 = query.get_statement(
    "IDC_0100_AEIM_001.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df = db.read_data("default", statement_001)
df.cache()
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,CREACION TABLA DELTA TEMP_DISP_IMSS_ISSSTE
df1 = df.select('FTC_FOLIO','FTN_ID_ARCHIVO','FTN_NSS','FTC_CURP','FTC_RFC','FTC_NOMBRE_CTE','FTN_ESTATUS_DIAG','FTC_CLAVE_ENT_RECEP','FTC_CLAVE_TMP', 'FNC_REG_PATRONAL_IMSS')
temp_view = 'TEMP_DISP_IMSS_ISSSTE' + '_' + params.sr_id_archivo
db.write_delta(temp_view, df1, "overwrite")

if conf.debug:
    display(df1)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, from_utc_timestamp, lit, col

df2 = df.select(
    'FTC_FOLIO',
    col('FTC_CLAVE_TMP').alias('FTC_NSS_CURP'),
    to_timestamp(from_utc_timestamp(current_timestamp(), 'GMT-6')).alias('FTD_FEH_CRE'),
    lit(params.sr_usuario).alias('FTC_USU_CRE')
)
display(df2)

# COMMAND ----------

statement_002 = query.get_statement(
    "IDC_0100_AEIM_002.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(
    statement=statement_002, async_mode=False
)

# COMMAND ----------

db.write_data(df2, "CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS", "default", "append")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())