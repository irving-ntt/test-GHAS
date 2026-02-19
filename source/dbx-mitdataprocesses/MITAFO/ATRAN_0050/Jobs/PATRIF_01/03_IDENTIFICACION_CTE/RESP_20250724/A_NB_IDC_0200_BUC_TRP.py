# Databricks notebook source


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
            col("Archivo SQL").startswith("SQL_IDC_200")
        )
    )

# COMMAND ----------

# DBTITLE 1,Construcción SQL IMSS
statement_001 = query.get_statement(
    "SQL_IDC_200_TRP_001.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_001)

if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA DELTA 001
temp_view = 'DELTA_200_TRP_001_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")

if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,CONSTRUCCION SQL INFONAVIT
statement_001 = query.get_statement(
    "SQL_IDC_200_TRP_002.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_001)

if conf.debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA DELTA 002
temp_view = 'DELTA_200_TRP_002_' + params.sr_folio
db.write_delta(temp_view, db.read_data("default", statement_001), "overwrite")

if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,EJECUTA QUERY 001 para TABLA DELTA_102_TRP
statement_2 = query.get_statement(
    "SQL_DELTA_TRP_001.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_102_TRP_{params.sr_folio}"
)

df_delta001 = db.sql_delta(statement_2)

if conf.debug:
    display(df_delta001)

# COMMAND ----------

# DBTITLE 1,CREA TABLA DELTA TMP001
df_delta001.write.format("delta").mode("overwrite").saveAsTable(f"DELTA_200_TMP_001_{params.sr_folio}")

df_from_delta_001=db.read_delta(f"DELTA_200_TMP_001_{params.sr_folio}")

if conf.debug:
    display(df_from_delta_001)

# COMMAND ----------

# DBTITLE 1,EJECUTA QUERY 002 TABLA DELTA_102_TRP
statement_3 = query.get_statement(
    "SQL_DELTA_TRP_002.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_102_TRP_{params.sr_folio}"
)

df_delta002 = db.sql_delta(statement_3)

if conf.debug:
    display(df_delta002)

# COMMAND ----------

# DBTITLE 1,CCREA TABLA DELTA TMP002
df_delta002.write.format("delta").mode("overwrite").saveAsTable(f"DELTA_200_TMP_002_{params.sr_folio}")

df_from_delta_001=db.read_delta(f"DELTA_200_TMP_002_{params.sr_folio}")

if conf.debug:
    display(df_from_delta_001)

# COMMAND ----------

statement_003 = query.get_statement(
    "SQL_JOIN_200_TRP_001.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}"
)

df_union_1 = db.sql_delta(statement_003)

if conf.debug:
    display(df_union_1)

# COMMAND ----------

# DBTITLE 1,EJECUTA JOIN PARA CURP
statement_004 = query.get_statement(
    "SQL_JOIN_200_TRP_002.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}"
)

df_union_2 = db.sql_delta(statement_004)

if conf.debug:
    display(df_union_2)

# COMMAND ----------

# DBTITLE 1,UNE EL RESULTADO DE JOINS PREVIOS
union_df = df_union_1.union(df_union_2)

if conf.debug:
    display(union_df)

# COMMAND ----------

# DBTITLE 1,ESCRIBE TABLA DELTA DE SALIDA
union_df.write.format("delta").mode("overwrite").saveAsTable(f"DELTA_111_TRAMITE_{params.sr_folio}")

df_final=db.read_delta(f"DELTA_111_TRAMITE_{params.sr_folio}")

if conf.debug:
    display(df_final)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
