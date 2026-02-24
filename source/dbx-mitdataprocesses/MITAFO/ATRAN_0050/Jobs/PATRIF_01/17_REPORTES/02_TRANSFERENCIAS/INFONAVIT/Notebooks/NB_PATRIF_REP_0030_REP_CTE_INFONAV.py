# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0030_REP_CTE_INFONAV
# MAGIC
# MAGIC **Descripción:** Poblar la tabla de catálogo de clientes INFONAVIT (TDAFOTRAS_CAT_CTE_INFONAVIT) en el esquema DATAMARTS. El notebook lee información de clientes desde un dataset Delta generado previamente por el job JP_PATRIF_REP_0020_REP_TRANSFERENCIAS, elimina duplicados y realiza un MERGE (upsert) en la tabla de catálogo usando el NSS como clave.
# MAGIC
# MAGIC **Subetapa:** 4407
# MAGIC
# MAGIC **Trámite:** 97 - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - DELTA_CLIENTES_{sr_folio} (Delta - Dataset de clientes generado por job anterior)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TDAFOTRAS_CAT_CTE_INFONAVIT (Oracle - Tabla de catálogo de clientes INFONAVIT)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_CLIENTES_{sr_folio}
# MAGIC - TEMP_CLIENTES_DEDUP_{sr_folio}
# MAGIC
# MAGIC **Tablas Auxiliares:**
# MAGIC - CIERREN_DATAUX.TDAFOTRAS_CAT_CTE_INFONAVIT_AUX (Oracle - Tabla auxiliar para MERGE)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAV_001_DELTA_READ_DATASET.sql
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAV_002_DELTA_DEDUP.sql
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAV_003_OCI_MERGE.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **[DS_500_CTES]**: Leer información de clientes desde un archivo dataset generado previamente por el job JP_PATRIF_REP_0020_REP_TRANSFERENCIAS
# MAGIC 2. **[RD300_QUITA_DUP]**: Eliminar registros duplicados basándose en el campo FTC_NSS_INFONA, manteniendo el primer registro encontrado
# MAGIC 3. **[BD_300_TDCRXGRAL_CAT_CLIENTE]**: Realizar UPDATE/INSERT (upsert) en la tabla de catálogo de clientes INFONAVIT usando FTC_NSS_INFONA como clave primaria

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams(
    {
        # Parámetros obligatorios del framework
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_id_snapshot": str,
        # Parámetros dinámicos que realmente se usan en este notebook
        "sr_folio": str,
        "sr_subproceso": str,
    }
)
conf = ConfManager()
params.validate()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())
logger.info(f"Iniciando procesamiento para folio: {params.sr_folio}")

# COMMAND ----------

# DBTITLE 1,Lectura de dataset de clientes desde Delta
statement_001 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAV_001_DELTA_READ_DATASET.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_clientes = db.sql_delta(statement_001)
db.write_delta(
    f"TEMP_CLIENTES_{params.sr_folio}",
    df_clientes,
    "overwrite",
)
if conf.debug:
    display(df_clientes)
logger.info(f"Clientes leídos desde dataset: {df_clientes.count()} registros")

# COMMAND ----------

# DBTITLE 1,Eliminación de duplicados por NSS
statement_002 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAV_002_DELTA_DEDUP.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_dedup = db.sql_delta(statement_002)
db.write_delta(
    f"TEMP_CLIENTES_DEDUP_{params.sr_folio}",
    df_dedup,
    "overwrite",
)
if conf.debug:
    display(df_dedup)
logger.info(f"Duplicados eliminados: {df_dedup.count()} registros únicos")

# COMMAND ----------

# DBTITLE 1,Carga de datos en tabla auxiliar
db.write_data(
    df_dedup,
    f"{conf.CX_DATAUX_ESQUEMA}.{conf.TL_DTM_CAT_CTE_INFONAVIT_AUX}",
    "default",
    "append",
)
logger.info(f"Datos cargados en tabla auxiliar: {df_dedup.count()} registros")

# COMMAND ----------

# DBTITLE 1,MERGE en tabla de catálogo de clientes
statement_003 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAV_003_OCI_MERGE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT=conf.TL_DTM_CAT_CTE_INFONAVIT,
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT_AUX=conf.TL_DTM_CAT_CTE_INFONAVIT_AUX,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=statement_003, async_mode=False)
logger.info(f"MERGE ejecutado: {execution}")

# COMMAND ----------

# DBTITLE 1,Limpieza de tabla auxiliar
statement_004 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAV_004_OCI_DELETE_AUX.sql",
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT_AUX=conf.TL_DTM_CAT_CTE_INFONAVIT_AUX,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=statement_004, async_mode=False)
logger.info(f"DELETE tabla auxiliar ejecutado: {execution}")


# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
logger.info("Procesamiento completado exitosamente")
