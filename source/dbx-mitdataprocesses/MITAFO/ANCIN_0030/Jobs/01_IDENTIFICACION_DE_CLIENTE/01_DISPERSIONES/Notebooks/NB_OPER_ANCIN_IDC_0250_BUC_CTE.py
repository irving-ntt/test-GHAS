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
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0250_BUC_CTE")))

# COMMAND ----------

temp_var = ""
if params.var_tramite == "IMSS":
    temp_var = "FTC_NSS"
elif params.var_tramite == "ISSSTE":
    temp_var = "FTC_CURP"
elif params.var_tramite == "AEIM":
    temp_var = "FTC_NSS"
else:
    raise Exception("No se ha definido el parámetro var_tramite")

# COMMAND ----------

statement_001 = query.get_statement(
    "IDC_0250_BUC_CTE_001.sql",
    VAR_CURP_NSS=temp_var,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)

# COMMAND ----------

temp_view = 'TEMP_INDEN_BUC' + '_' + params.sr_id_archivo
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))


# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())