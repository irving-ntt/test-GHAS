# Databricks notebook source
# MAGIC %sql
# MAGIC /*
# MAGIC %md
# MAGIC TRAPASOS:
# MAGIC NoteBook que genera el Encabezado del archivo de respuesta del SubPoceso 3286 - Transferencia de Recursos por Portabilidad
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
        "sr_id_archivo": str,
        "sr_fec_arc": str,
        "sr_paso": str,
        "sr_id_archivo_06": str,
        "sr_id_archivo_09": str
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("GAR")))

# COMMAND ----------

# DBTITLE 1,Construcción Encabezado
statement_001 = query.get_statement(
    "GAR_0100_ENCABEZADO.sql",
    SR_FOLIO = params.sr_folio
)

# COMMAND ----------

# DBTITLE 1,Ejecución Encabezado
if conf.debug:
   df = db.read_data("default", statement_001)
if conf.debug:
    display(df)
    display(str(df.count()) + " Registros Totales")

# COMMAND ----------

# DBTITLE 1,Generacion de archivo de Salida
temp_delta = 'DELTA_GAR_0100_ENCABEZADO' + '_' + params.sr_folio
db.write_delta(temp_delta, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_delta))

# COMMAND ----------

# DBTITLE 1,Depuración de Dataframes usados
CleanUpManager.cleanup_notebook(locals())
