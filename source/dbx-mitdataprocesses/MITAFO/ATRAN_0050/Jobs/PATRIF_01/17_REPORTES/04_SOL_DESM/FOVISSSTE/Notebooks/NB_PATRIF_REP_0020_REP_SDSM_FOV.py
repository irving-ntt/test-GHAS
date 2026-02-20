# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0020_REP_SDSM_FOV
# MAGIC
# MAGIC **Descripción:** Job paralelo que carga datos de desmarca FOVISSSTE desde la tabla PROCESOS.TTAFOTRAS_DESMARCA_FOVST hacia la tabla de reporte DATAMARTS.TMAFOTRAS_DESMARCA_FOVI para su uso en reportes de Cognos.
# MAGIC
# MAGIC **Subetapa:** Reportes de Desmarca FOVISSSTE
# MAGIC
# MAGIC **Trámite:** Reportes de Desmarca
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_DESMARCA_FOVST (Oracle - tabla de origen)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_DESMARCA_FOVI (Oracle - tabla de reporte)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_DSM_FOV_{sr_folio}
# MAGIC - TEMP_TRANSFORM_DSM_FOV_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0020_REP_SDSM_FOV_001_OCI_EXTRACCION.sql
# MAGIC - PATRIF_REP_0020_REP_SDSM_FOV_002_DELTA_TRANSFORM.sql
# MAGIC - PATRIF_REP_0020_REP_SDSM_FOV_003_OCI_DELETE.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100_DSM_FOV**: Extraer datos de desmarca FOVISSSTE desde Oracle a Delta
# MAGIC 2. **TF_400_MOD_TIPODATO**: Aplicar transformaciones de tipos de datos y agregar campos de auditoría con SQL desde Delta
# MAGIC 3. **DB_500_SDSM_FOV_DATA**: Ejecutar DELETE previo y luego INSERT directo en tabla de destino Oracle

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

# Cargar configuración de entorno
conf = ConfManager()

# Validar parámetros
params.validate()

# Inicializar managers
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,DB_100_DSM_FOV - Extracción desde Oracle
# Leer datos desde Oracle y escribir en Delta
statement_001 = query.get_statement(
    "PATRIF_REP_0020_REP_SDSM_FOV_001_OCI_EXTRACCION.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_DESM_FOV=conf.TL_PRO_DESM_FOV,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

db.write_delta(
    f"TEMP_DSM_FOV_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite"
)

if conf.debug:
    display(db.read_delta(f"TEMP_DSM_FOV_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_DSM_FOV_{params.sr_folio}"))

logger.info("Datos extraídos desde Oracle y guardados en Delta")

# COMMAND ----------

# DBTITLE 2,TF_400_MOD_TIPODATO - Transformación de Tipos de Datos
# Aplicar transformaciones usando SQL desde Delta
statement_002 = query.get_statement(
    "PATRIF_REP_0020_REP_SDSM_FOV_002_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO
)

db.write_delta(
    f"TEMP_TRANSFORM_DSM_FOV_{params.sr_folio}",
    db.sql_delta(statement_002),
    "overwrite"
)

if conf.debug:
    display(db.read_delta(f"TEMP_TRANSFORM_DSM_FOV_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_TRANSFORM_DSM_FOV_{params.sr_folio}"))

logger.info("Transformaciones aplicadas exitosamente")

# COMMAND ----------

# DBTITLE 3,DB_500_SDSM_FOV_DATA - DELETE Previo y INSERT en Oracle
# Ejecutar DELETE previo en la tabla destino
statement_003 = query.get_statement(
    "PATRIF_REP_0020_REP_SDSM_FOV_003_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_DESM_FOV=conf.TL_DTM_DESM_FOV,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */"
)

db.execute_oci_dml(statement=statement_003, async_mode=False)

logger.info("DELETE previo ejecutado exitosamente")

# COMMAND ----------

# Insertar datos directamente en la tabla destino (WriteMode=0, NO tabla auxiliar)
df_resultado = db.read_delta(f"TEMP_TRANSFORM_DSM_FOV_{params.sr_folio}")

db.write_data(
    df_resultado,
    f"{conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_DESM_FOV}",
    "default",
    "append"
)

logger.info("INSERT ejecutado exitosamente en Oracle")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
