# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Carga
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     3832 - Devolución de Pagos sin Justificación Legal
# MAGIC Querys:
# MAGIC     SQL_IDC_702_VAL_IDENT_SUM.sql
# MAGIC     SQL_IDC_700_DELETE_SUM.sql
# MAGIC Deltas:
# MAGIC     
# MAGIC Tablas
# MAGIC     PROCESOS.TTCRXGRAL_DEV_PSJL_SUMARIZADOS
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
            col("Archivo SQL").startswith("SQL_IDC_700")
        )
    )

# COMMAND ----------

# DBTITLE 1,GENERA  DELTA_702_VAL_IDENT_DUP
statement_1 = query.get_statement(
    "SQL_IDC_702_VAL_IDENT_SUM.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
#df = db.read_data("default", statement_1)

#if conf.debug:
    #display(df)




# COMMAND ----------

# DBTITLE 1,BORRADO PREVIO DE TTCRXGRAL_DEV_PAG_SJL_SUM
statement_4 = query.get_statement(
    "SQL_IDC_700_DELETE_SUM.sql",
    SR_FOLIO=params.sr_folio
)
execution = db.execute_oci_dml(
    statement=statement_4, async_mode=False
)

# COMMAND ----------

statement_4 = query.get_statement(
    "SQL_IDC_702_VAL_IDENT_SUM.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
execution = db.execute_oci_dml(
    statement=statement_4, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,CARGA TABLA TTCRXGRAL_DEV_PAG_SJL_SUMARIZADOS
#db.write_data(df, "PROCESOS.TTCRXGRAL_DEV_PSJL_SUMARIZADOS", "default", "append")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
