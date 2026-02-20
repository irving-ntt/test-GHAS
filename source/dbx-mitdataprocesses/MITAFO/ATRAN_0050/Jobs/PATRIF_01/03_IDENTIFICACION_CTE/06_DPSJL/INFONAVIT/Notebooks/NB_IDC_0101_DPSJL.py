# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Extracción inicial
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     3832 - Devolución de Pagos sin Justificación Legal
# MAGIC Querys:
# MAGIC     SQL_IDC_100_DPSJL.sql
# MAGIC Deltas:
# MAGIC     DELTA_300_DPSJL
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,INICIO - CONFIGURACIONES PRINCIPALES
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,PARAMETROS
# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({

    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_proceso": str,
    "sr_subetapa": str,
    "sr_dt_org_arc": str,
    "sr_origen_arc": str,
    "sr_subproceso": str,
    "sr_tipo_layout": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso": str,
})

# Validar widgets
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()


# COMMAND ----------

# DBTITLE 1,VALIDA EXISTENCIA SQL

queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
if conf.debug:
    display(
        queries_df.filter(
            col("Archivo SQL").startswith("SQL_IDC_100")
        )
    )

# COMMAND ----------

# DBTITLE 1,VERIFICA SUBPROCESO ACTIVO
SUBPROCESO = dbutils.widgets.get("sr_subproceso")

if SUBPROCESO == '3832':
    Query = "SQL_IDC_100_DPSJL.sql"
    Delta = "DELTA_300_DPSJL"
    
display("Q: " + Query + '  D: ' + Delta)



# COMMAND ----------

# DBTITLE 1,CONSTRUCCION QUERY SEGUN SUBPROCESO
statement_001 = query.get_statement(
    #Query DEFINICION DEPENDE DEL SUBPROCESO,
    Query,
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_001)

if conf.debug:
    display(df)


# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA DELTA DEL PROCESO 100
#Delta DEFINICION DEPENDE DEL SUBPROCESO
temp_view = Delta + '_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")
if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
