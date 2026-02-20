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
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,Extracción y creación de tabla delta
statement_001 = query.get_statement(
    "IDC_0600_UPD_IMSSSTE_CTE.sql",
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

db.write_delta('TEMP_CTAS_VIG_CARGA' + '_' + params.sr_id_archivo, db.sql_delta(statement_001), "overwrite")

if conf.debug:
    display(db.read_delta('TEMP_CTAS_VIG_CARGA' + '_' + params.sr_id_archivo))

# COMMAND ----------

# DBTITLE 1,Liberación de recursos
CleanUpManager.cleanup_notebook(locals())
