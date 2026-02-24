# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0040_REP_CTE_DPSJL
# MAGIC
# MAGIC **Descripción:** Job que carga la tabla de catálogo de clientes Infonavit desde la tabla de procesos de devolución de pagos sin justificación legal. Extrae información de clientes únicos por NSS, aplica transformaciones de limpieza y mapeo, elimina duplicados, y finalmente actualiza o inserta los registros en la tabla de catálogo DATAMARTS.TDAFOTRAS_CAT_CTE_INFONAVIT.
# MAGIC
# MAGIC **Subetapa:** Actualización de catálogo de clientes Infonavit
# MAGIC
# MAGIC **Trámite:** Reportes de Devolución de Pagos Sin Justificación Legal
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_DEV_PAG_SJL (Oracle - tabla de procesos de devolución de pagos sin justificación legal)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TDAFOTRAS_CAT_CTE_INFONAVIT (Oracle - tabla de catálogo de clientes Infonavit)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_PROCESOS_#SR_FOLIO#
# MAGIC - TEMP_TRIM_#SR_FOLIO#
# MAGIC - TEMP_DEDUP_#SR_FOLIO#
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0040_REP_CTE_DPSJL_001_OCI_PROCESOS.sql
# MAGIC - PATRIF_REP_0040_REP_CTE_DPSJL_002_DELTA_TRIM.sql
# MAGIC - PATRIF_REP_0040_REP_CTE_DPSJL_003_DELTA_DEDUP.sql
# MAGIC - PATRIF_REP_0040_REP_CTE_DPSJL_004_OCI_MERGE.sql
# MAGIC - PATRIF_REP_0040_REP_CTE_DPSJL_005_OCI_DELETE_AUX.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100_PROCESOS**: Extraer información de clientes desde Oracle con SELECT DISTINCT
# MAGIC 2. **TF_200_MAPEO**: Aplicar TRIM y mapeo de campos
# MAGIC 3. **RD_300_NSS**: Eliminar duplicados por FTC_NSS_INFONA
# MAGIC 4. **BD_300_TDCRXGRAL_CAT_CLIENTE**: Cargar datos en tabla auxiliar, ejecutar MERGE y limpiar tabla auxiliar

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
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
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,DB_100_PROCESOS - Extracción desde Oracle
# Leer información de clientes desde Oracle con SELECT DISTINCT
statement_001 = query.get_statement(
    "PATRIF_REP_0040_REP_CTE_DPSJL_001_OCI_PROCESOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_DEV_PAG_SJL=conf.TL_PRO_DEV_PAG_SJL,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# Escribir datos en Delta temporal
db.write_delta(
    f"TEMP_PROCESOS_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_PROCESOS_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_PROCESOS_{params.sr_folio}"))
logger.info("Datos extraídos desde Oracle y guardados en Delta")

# COMMAND ----------

# DBTITLE 2,TF_200_MAPEO - Aplicar TRIM y mapeo
# Aplicar transformaciones de limpieza y mapeo de campos
statement_002 = query.get_statement(
    "PATRIF_REP_0040_REP_CTE_DPSJL_002_DELTA_TRIM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# Escribir datos transformados en Delta
db.write_delta(
    f"TEMP_TRIM_{params.sr_folio}",
    db.sql_delta(statement_002),
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_TRIM_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_TRIM_{params.sr_folio}"))
logger.info("Transformaciones aplicadas y guardadas en Delta")

# COMMAND ----------

# DBTITLE 3,RD_300_NSS - Eliminar duplicados
# Eliminar duplicados basados en el campo FTC_NSS_INFONA
statement_003 = query.get_statement(
    "PATRIF_REP_0040_REP_CTE_DPSJL_003_DELTA_DEDUP.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# Escribir datos deduplicados en Delta
df_dedup = db.sql_delta(statement_003)
db.write_delta(
    f"TEMP_DEDUP_{params.sr_folio}",
    df_dedup,
    "overwrite"
)
if conf.debug:
    display(df_dedup.count())
    display(df_dedup)
logger.info("Deduplicación completada y guardada en Delta")

# COMMAND ----------

# DBTITLE 4,BD_300_TDCRXGRAL_CAT_CLIENTE - Cargar en tabla auxiliar
# Cargar datos deduplicados en tabla auxiliar para MERGE
db.write_data(
    df_dedup,
    f"{conf.CX_DATAUX_ESQUEMA}.{conf.TL_DTM_CAT_CTE_INFONAVIT_AUX}",
    "default",
    "append",
)
logger.info(f"Datos cargados en tabla auxiliar")

# COMMAND ----------

# DBTITLE 5,MERGE en tabla de catálogo de clientes
# Ejecutar MERGE desde tabla auxiliar hacia tabla final
statement_004 = query.get_statement(
    "PATRIF_REP_0040_REP_CTE_DPSJL_004_OCI_MERGE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT=conf.TL_DTM_CAT_CTE_INFONAVIT,
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT_AUX=conf.TL_DTM_CAT_CTE_INFONAVIT_AUX,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(statement=statement_004, async_mode=False)

# COMMAND ----------

# DBTITLE 6,Limpiar tabla auxiliar
# Limpiar tabla auxiliar después del MERGE
statement_005 = query.get_statement(
    "PATRIF_REP_0040_REP_CTE_DPSJL_005_OCI_DELETE_AUX.sql",
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT_AUX=conf.TL_DTM_CAT_CTE_INFONAVIT_AUX,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(statement=statement_005, async_mode=False)
logger.info("MERGE ejecutado y tabla auxiliar limpiada exitosamente")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
