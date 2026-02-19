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

# DBTITLE 1,Construcción de Consulta Principal
statement_001 = query.get_statement(
    "IDC_0000_EXTRA_NSS_CURP_001.sql",
    SR_FOLIO=params.sr_folio,
)


# COMMAND ----------

# DBTITLE 1,Creación de tabla delta
db.write_delta('TEMP_VAL_IDENT_CIENTE_01' + '_' + params.sr_id_archivo, db.read_data("default", statement_001), "overwrite")

if conf.debug:
    display(db.read_delta('TEMP_VAL_IDENT_CIENTE_01' + '_' + params.sr_id_archivo))

# COMMAND ----------

# DBTITLE 1,Liberación de Recursos
CleanUpManager.cleanup_notebook(locals())
