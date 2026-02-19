# Databricks notebook source
# DBTITLE 1,Inicio - Configuraciones Principales
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parámetros
params = WidgetParams(
    {
        "sr_folio": str,
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_subetapa": str,
        "sr_sec_lote": str,
        "sr_fecha_lote": str,
        "sr_fecha_acc": str,
        "sr_tipo_archivo": str,
        "sr_estatus_mov": str,
        "sr_tipo_mov": str,
        "sr_accion": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
    }
)
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
queries_df = query.get_sql_list()
# filtar solo los registros donde la columan 'Archivo SQL' comiencen por el nombre del notebook
display(queries_df.filter(col("Archivo SQL").startswith("GAR_0010_GEN_ARCH")))

# COMMAND ----------

# DBTITLE 1,Construcción SQL Unión Mov e Indicadores
statement_001 = query.get_statement(
    "GAR_0010_GEN_ARCH_CTINDI_UNION_004.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# COMMAND ----------

# DBTITLE 1,Ejecución SQL Unión Mov e Indicadores
df_union = db.sql_delta(statement_001)
if conf.debug:
    display(df_union)

# COMMAND ----------

# DBTITLE 1,Construcción y Llenado Tabla Delta
temp_view = 'DELTA_DOIMSS_CTI_04_UNION_' + params.sr_folio
db.write_delta(temp_view, db.sql_delta(statement_001), "overwrite")

if conf.debug:
    display(db.read_delta(temp_view))

# COMMAND ----------

# DBTITLE 1,Construcción SQL Consulta Final
statement_002 = query.get_statement(
    "GAR_0010_GEN_ARCH_CTINDI_UNION_005.sql",
    SR_FOLIO=params.sr_folio,
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
)

# COMMAND ----------

# DBTITLE 1,Ejecución de Consulta Final
df_union_final = db.sql_delta(statement_002)
if conf.debug:
    display(df_union_final)

# COMMAND ----------

# DBTITLE 1,Construcción de Delete
statement_003 = query.get_statement(
    "GAR_0010_GEN_ARCH_CTINDI_DELETE_006.sql",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 1,Ejecución de Delete en OCI
execution = db.execute_oci_dml(
    statement=statement_003, async_mode=False
)

# COMMAND ----------

# DBTITLE 1,Inserción de datos en tabla OCI
#CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO

db.write_data(df_union_final, "CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO", "default", "append")

# COMMAND ----------

# DBTITLE 1,Fin - Limpieza
CleanUpManager.cleanup_notebook(locals())