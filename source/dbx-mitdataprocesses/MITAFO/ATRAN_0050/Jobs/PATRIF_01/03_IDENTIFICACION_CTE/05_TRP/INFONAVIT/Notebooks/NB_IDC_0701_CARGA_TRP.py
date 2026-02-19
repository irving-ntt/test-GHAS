# Databricks notebook source
# DBTITLE 1,DESCRIPCIÓN GENERAL
# MAGIC %md
# MAGIC Descripcion:
# MAGIC     Carga información en tabla OCI
# MAGIC Subetapa:
# MAGIC     IDC - Identificación del Cliente
# MAGIC Trámite:
# MAGIC     354  - Solicitud de Marca de Cuentas por 43 BIS
# MAGIC Tablas INPUT:
# MAGIC     N/A
# MAGIC Tablas OUTPUT:
# MAGIC     PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
# MAGIC Tablas INPUT DELTA:
# MAGIC     DELTA_VIVIENDA
# MAGIC 	DELTA_DUPLICADOS
# MAGIC Tablas OUTPUT DELTA:
# MAGIC     N/A
# MAGIC Archivos SQL:
# MAGIC     SQL_IDC_700_JOIN_VIV.sql
# MAGIC     SQL_IDC_700_MARCA.sql
# MAGIC     SQL_IDC_700_DELETE.sql
# MAGIC     SQL_IDC_700_VAL_IDENT.sql

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

# DBTITLE 1,GENERA  DELTA_700_VAL_IDENT
statement_1 = query.get_statement(
    "SQL_IDC_700_VAL_IDENT2.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_1)

if conf.debug:
    display(df)

db.write_delta(f"DELTA_700_VAL_IDENT2_{params.sr_folio}", df, "overwrite")


# COMMAND ----------

# DBTITLE 1,GENERA DELTA CON JOIN DE DELTA BUC, DUPLICADOS
statement_2 = query.get_statement(
    "SQL_IDC_700_JOIN_BUC.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    DELTA_VAL_IDENT=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_700_VAL_IDENT2_{params.sr_folio}",
    DELTA_BUC=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_BUC_DATOS_{params.sr_folio}",
    DELTA_DUP=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_DUPLICADOS_{params.sr_folio}",
)

df_join_viv_702 = db.sql_delta(statement_2)

if conf.debug:
    display(df_join_viv_702)

db.write_delta(f"DELTA_700_BUC2_{params.sr_folio}", df_join_viv_702, "overwrite")


# COMMAND ----------

# DBTITLE 1,UNION DE 700 y 701
statement_curp = query.get_statement(
    "SQL_IDC_700_NSS.sql",
    DELTA_TABLA_NAME=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_700_NSS_{params.sr_folio}"
)
df_encontrados_nss = db.sql_delta(statement_curp)

df_union = df_join_viv_702.union(df_encontrados_nss)

if conf.debug:
    display(df_union)

db.write_delta(f"DELTA_701_ENCONTRADOS_{params.sr_folio}", df_union, "overwrite")



# COMMAND ----------

# DBTITLE 1,Aplica reglas de TRANSFORMACION
statement_3 = query.get_statement(
    "SQL_IDC_700_MARCA.sql",
    DELTA_701=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_701_ENCONTRADOS_{params.sr_folio}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

df_marca_701 = db.sql_delta(statement_3)

db.write_delta(f"DELTA_700_SALIDA_{params.sr_folio}", df_marca_701, "overwrite")

if conf.debug:
    display(df_marca_701)



# COMMAND ----------

# DBTITLE 1,BORRADO PREVIO DE PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
statement_4 = query.get_statement(
    "SQL_IDC_700_DELETE.sql",
    SR_FOLIO=params.sr_folio
)
execution = db.execute_oci_dml(
    statement=statement_4, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,CARGA TABLA PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
statement_4 = query.get_statement(
    "SQL_IDC_700_CARGA_PORTA.sql",
    DELTA_CARGA_1=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_700_SALIDA_{params.sr_folio}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

df_carga_porta = db.sql_delta(statement_4)

db.write_data(df_carga_porta, "PROCESOS.TTAFOTRAS_TRANS_REC_PORTA", "default", "append")

if conf.debug:
    display(df_carga_porta)



# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
