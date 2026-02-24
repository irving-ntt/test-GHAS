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
    "IDC_0100_MAP_IMSS_ISSSTE_001.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo
)

# COMMAND ----------

db.write_delta(f"TEMP_MAP_IMSS_ISSSTE_{params.sr_id_archivo}", db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(f"TEMP_MAP_IMSS_ISSSTE_{params.sr_id_archivo}"))

# COMMAND ----------

statement_002 = query.get_statement(
    "IDC_0100_MAP_IMSS_ISSSTE_002.sql",
    VAR_TRAMITE=params.var_tramite,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    SR_USUARIO=params.sr_usuario,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# COMMAND ----------

df = db.sql_delta(statement_002)
df.cache()
if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,CREACION VISTA GLOBAL TEMP_DISP_IMSS_ISSSTE
df1 = df.select('FTC_FOLIO','FTN_ID_ARCHIVO','FTN_NSS','FTC_CURP','FTC_RFC','FTC_NOMBRE_CTE','FTC_CLAVE_ENT_RECEP','FTC_CLAVE_TMP')
temp_view = 'TEMP_DISP_IMSS_ISSSTE' + '_' + params.sr_id_archivo
db.write_delta(temp_view, df1, "overwrite")

if conf.debug:
    display(df1)

# COMMAND ----------

df2 = df.select('FTC_FOLIO','FTC_NSS_CURP','FTD_FEH_CRE','FTC_USU_CRE')


# COMMAND ----------

statement_003 = query.get_statement(
    "IDC_0100_MAP_IMSS_ISSSTE_003.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

db.write_data(df2, "CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS", "default", "append")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())