# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0030_REP_CTE_INFONAVIT
# MAGIC
# MAGIC **Descripción:** Job paralelo que carga el catálogo de clientes INFONAVIT desde la tabla PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO, aplica transformaciones, elimina duplicados y actualiza o inserta registros en la tabla de catálogo de clientes INFONAVIT en Oracle.
# MAGIC
# MAGIC **Subetapa:** Carga de catálogo de clientes INFONAVIT
# MAGIC
# MAGIC **Trámite:** Reportes de marca/desmarca INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO (Oracle - información de marca/desmarca INFONAVIT-FOVISSSTE)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TDAFOTRAS_CAT_CTE_INFONAVIT (Oracle - catálogo de clientes INFONAVIT)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_MARCA_DESMARCA_INFO_{sr_folio}
# MAGIC - TEMP_MAPEO_{sr_folio}
# MAGIC - TEMP_REMDUP_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAVIT_001_OCI_READ.sql
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAVIT_002_DELTA_TRANSFORM.sql
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAVIT_003_DELTA_REMDUP.sql
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAVIT_004_OCI_MERGE.sql
# MAGIC - PATRIF_REP_0030_REP_CTE_INFONAVIT_006_OCI_DELETE_AUX.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100_PROCESOS**: Extraer datos desde Oracle (PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO)
# MAGIC 2. **TF_200_MAPEO**: Aplicar transformaciones (TRIM y mapeo de campos)
# MAGIC 3. **RD_300_NSS**: Eliminar duplicados por FTC_NSS_INFONA
# MAGIC 4. **BD_300_TDCRXGRAL_CAT_CLIENTE**: MERGE en Oracle (DATAMARTS.TDAFOTRAS_CAT_CTE_INFONAVIT)

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams({
    "sr_folio": str,
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})

# Cargar configuración de entorno
conf = ConfManager()

# Validar parámetros
params.validate()

# Inicializar managers
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,DB_100_PROCESOS - Extracción desde Oracle
logger.info(f"Iniciando extracción de datos desde Oracle para folio: {params.sr_folio}")
statement_001 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAVIT_001_OCI_READ.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_MARCA_DESMARCA_INFO=conf.TL_PRO_MARCA_DESMARCA_INFO,
    SR_FOLIO=params.sr_folio
)
df_raw_data = db.read_data("default", statement_001)
db.write_delta(
    f"TEMP_MARCA_DESMARCA_INFO_{params.sr_folio}",
    df_raw_data,
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_DESMARCA_INFO_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MARCA_DESMARCA_INFO_{params.sr_folio}"))
logger.info("Datos extraídos desde Oracle y guardados en Delta.")

# COMMAND ----------

# DBTITLE 1,TF_200_MAPEO - Aplicar Transformaciones
logger.info("Aplicando transformaciones iniciales.")
statement_002 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAVIT_002_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)
df_transformed = db.sql_delta(statement_002)
db.write_delta(
    f"TEMP_MAPEO_{params.sr_folio}",
    df_transformed,
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_MAPEO_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MAPEO_{params.sr_folio}"))
logger.info("Transformaciones aplicadas exitosamente.")

# COMMAND ----------

# DBTITLE 1,RD_300_NSS - Eliminar Duplicados
logger.info("Eliminando duplicados por FTC_NSS_INFONA.")
statement_003 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAVIT_003_DELTA_REMDUP.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)
df_deduplicated = db.sql_delta(statement_003)
db.write_delta(
    f"TEMP_REMDUP_{params.sr_folio}",
    df_deduplicated,
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_REMDUP_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_REMDUP_{params.sr_folio}"))
logger.info("Duplicados eliminados exitosamente.")

# COMMAND ----------

# DBTITLE 1,BD_300_TDCRXGRAL_CAT_CLIENTE - MERGE en Oracle
logger.info("Cargando datos deduplicados en tabla auxiliar de Oracle para MERGE.")
df_for_aux = db.read_delta(f"TEMP_REMDUP_{params.sr_folio}")
if conf.debug:
    display(df_for_aux.count())
    display(df_for_aux)

db.write_data(
    df_for_aux,
    f"{conf.CX_DATAUX_ESQUEMA}.{conf.TL_DTM_CAT_CTE_INFONAVIT_AUX}",
    "default",
    "append"
)
if conf.debug:
    count_aux = db.read_data("default", f"SELECT COUNT(*) AS CNT FROM {conf.CX_DATAUX_ESQUEMA}.{conf.TL_DTM_CAT_CTE_INFONAVIT_AUX}")
    display(count_aux)
logger.info("Datos insertados en tabla auxiliar Oracle.")

logger.info("Ejecutando operación MERGE en la tabla final de Oracle.")
statement_004 = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAVIT_004_OCI_MERGE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT=conf.TL_DTM_CAT_CTE_INFONAVIT,
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT_AUX=conf.TL_DTM_CAT_CTE_INFONAVIT_AUX
)
db.execute_oci_dml(
    statement=statement_004,
    async_mode=False
)

logger.info("Limpiando tabla auxiliar de Oracle después de MERGE.")
statement_delete_aux = query.get_statement(
    "PATRIF_REP_0030_REP_CTE_INFONAVIT_006_OCI_DELETE_AUX.sql",
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CTE_INFONAVIT_AUX=conf.TL_DTM_CAT_CTE_INFONAVIT_AUX
)
db.execute_oci_dml(
    statement=statement_delete_aux,
    async_mode=False
)

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
