# Databricks notebook source
# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("IDC_0220_BUC_DICTAMEN")))

# COMMAND ----------

# DBTITLE 1,Construcción ETL AIEM
statement_001 = query.get_statement(
    "IDC_0240_ACT_HISTORICO_004.sql",
    SR_FOLIO=params.sr_folio
)

# COMMAND ----------

# DBTITLE 1,Ejecución ETL AIEM
df = db.read_data("default", statement_001)
if conf.debug:
    display(df)
    display(str(df.count()) + " Registros Totales")

# COMMAND ----------

# DBTITLE 1,Llenado tabla AUX
table_name_aux = "CIERREN_DATAUX.TTSISGRAL_ETL_AEIM_AUX"
db.write_data(df, table_name_aux, "default", "append")

# COMMAND ----------

# DBTITLE 1,Construcción Merge Insert Update
statement_002 = query.get_statement(
    "IDC_0240_ACT_PROCESOS_005.sql",
    SR_FOLIO=params.sr_folio, SR_USUARIO=params.sr_usuario
)

# COMMAND ----------

# DBTITLE 1,Ejecución Merge Insert Update
execution = db.execute_oci_dml(
    statement=statement_002, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Construcción Merge Update
statement_003 = query.get_statement(
    "IDC_0240_ACT_PROCESOS_006.sql",
    SR_FOLIO=params.sr_folio, SR_USUARIO=params.sr_usuario
)

# COMMAND ----------

# DBTITLE 1,Ejecución Merge Update
execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Construcción de DELETE Tabla AUX
statement_004 = query.get_statement(
    "IDC_0230_ACT_ESTATUS_003.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de borrado tabla AUX
# La ejecucion se envia en segundo plano con async_mode=True
execution = db.execute_oci_dml(
    statement=statement_004, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Notificación
Notify.send_notification("INFO", params) 

# COMMAND ----------

# DBTITLE 1,Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())