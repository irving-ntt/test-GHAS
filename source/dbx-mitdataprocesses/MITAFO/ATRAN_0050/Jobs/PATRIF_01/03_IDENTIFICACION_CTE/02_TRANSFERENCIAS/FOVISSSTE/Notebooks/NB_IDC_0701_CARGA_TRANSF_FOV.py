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
    "SQL_IDC_700_VAL_IDENT.sql",
    SR_FOLIO=params.sr_folio,
    SR_ID_ARCHIVO=params.sr_id_archivo,
    hints="/*+ PARALLEL(4) */"
)
df = db.read_data("default", statement_1)

if conf.debug:
    display(df)

db.write_delta(f"DELTA_700_VAL_IDENT_{params.sr_folio}", df, "overwrite")


# COMMAND ----------

# DBTITLE 1,GENERA DELTA CON JOIN DE DELTAS VIVIENDA y BUC_DATOS
statement_2 = query.get_statement(
    "SQL_IDC_700_JOIN_VIV.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    DELTA_VAL_IDENT=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_700_VAL_IDENT_{params.sr_folio}",
    DELTA_VIV=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_VIVIENDA_{params.sr_folio}",
    DELTA_BUC=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_600_CARGA_{params.sr_folio}"
)

df_join_viv_700 = db.sql_delta(statement_2)

if conf.debug:
    display(df_join_viv_700)

db.write_delta(f"DELTA_700_VIV_DUP_{params.sr_folio}", df_join_viv_700, "overwrite")


# COMMAND ----------

# DBTITLE 1,Aplica reglas de TRANSFORMACION
statement_3 = query.get_statement(
    "SQL_IDC_700_MARCA.sql",
    DELTA_VIV_DUP=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_700_VIV_DUP_{params.sr_folio}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

df_marca_700 = db.sql_delta(statement_3)

if conf.debug:
    display(df_marca_700)



# COMMAND ----------

# DBTITLE 1,BORRADO PREVIO DE TTCRXGRAL_TRANS_INFONA
statement_4 = query.get_statement(
    "SQL_IDC_700_DELETE.sql",
    SR_FOLIO=params.sr_folio
)
execution = db.execute_oci_dml(
    statement=statement_4, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,CARGA TABLA TTCRXGRAL_TRANS_INFONA
db.write_data(df_marca_700, "PROCESOS.TTAFOTRAS_TRANS_FOVISSSTE", "default", "append")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
