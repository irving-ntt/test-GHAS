# Databricks notebook source
# MAGIC %sql
# MAGIC /*
# MAGIC %md
# MAGIC TRAPASOS:
# MAGIC NoteBook que Genera el archivo de respuesta del SubPoceso 3286 - Transferencia de Recursos por Portabilidad
# MAGIC */

# COMMAND ----------

# DBTITLE 1,Inicio
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_folio": str,
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
display(queries_df.filter(col("Archivo SQL").startswith("GAR_0")))

# COMMAND ----------

# DBTITLE 1,Construcción Actualizacion de la tabla PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
statement_001 = query.get_statement(
    "GAR_0500_GEN_ARCHIVO_RESP.sql",
    DELTA_ENCABEZADO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0100_encabezado_{params.sr_folio}",
	DELTA_DETALLE=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0200_detalle_{params.sr_folio}",
	DELTA_SUMARIO=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.delta_gar_0300_sumario_{params.sr_folio}"
)

# COMMAND ----------

# DBTITLE 1,Ejecución Archivo de Respuesta
df = spark.sql(statement_001)  #Se uso asi por las tablas que se encuentran creadas dentro de databricks
#df = db.read_data("default", statement_001)  # Replace 'your_table_name' with the actual table name.
if conf.debug:
    display(df)
    display(str(df.count()) + " Registros Totales")

# COMMAND ----------

# DBTITLE 1,Notificación
Notify.send_notification("INFO", params) 

# COMMAND ----------

# DBTITLE 1,Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())
