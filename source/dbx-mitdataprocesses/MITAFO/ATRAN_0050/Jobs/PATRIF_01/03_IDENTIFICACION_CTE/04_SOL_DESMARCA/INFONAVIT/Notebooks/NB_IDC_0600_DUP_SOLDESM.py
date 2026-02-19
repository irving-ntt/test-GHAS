# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Duplicados
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     347 - Desmarca Crédito de Vivienda por 43 BIS
# MAGIC Querys:
# MAGIC     SQL_IDC_600_DUP_SOLDESM.sql
# MAGIC Deltas:
# MAGIC     DELTA_DUPLICADOS
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
            col("Archivo SQL").startswith("SQL_IDC_600")
        )
    )

# COMMAND ----------

# DBTITLE 1,EJECUTA QUERY PARA DELTA DUPLICADOS
statement_001 = query.get_statement(
    "SQL_IDC_600_DUP_SOLDESM.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_001)

if conf.debug:
    display(df)

db.write_delta(f"DELTA_DUPLICADOS_{params.sr_folio}", df, "overwrite")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
