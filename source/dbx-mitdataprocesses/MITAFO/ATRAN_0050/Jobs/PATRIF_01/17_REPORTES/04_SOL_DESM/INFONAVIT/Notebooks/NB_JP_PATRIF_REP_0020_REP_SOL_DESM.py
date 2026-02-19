# Databricks notebook source
# MAGIC %md
# MAGIC # NB_JP_PATRIF_REP_0020_REP_SOL_DESM
# MAGIC
# MAGIC **Descripción:** Job paralelo que pobla la tabla de MARCA_DESMARCA para reporte de Cognos. Extrae datos de la tabla de Infonavit-Fovvissste, aplica transformaciones de tipos de datos y mapeo de campos, y finalmente inserta los datos transformados en la tabla de reporte de marca/desmarca.
# MAGIC
# MAGIC **Subetapa:** Carga de reportes
# MAGIC
# MAGIC **Trámite:** Reportes PATRIF
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO (Oracle - Tabla de Infonavit-Fovvissste)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_MARCA_DESMARCA (Oracle - Tabla de Reporte de Marca/Desmarca)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_MARCA_DESMARCA_INFO_{sr_folio}
# MAGIC - TEMP_MARCA_DESMARCA_TRANSFORM_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0020_REP_SOL_DESM_001_OCI_EXTRACCION.sql
# MAGIC - PATRIF_REP_0020_REP_SOL_DESM_002_DELTA_TRANSFORM.sql
# MAGIC - PATRIF_REP_0020_REP_SOL_DESM_003_OCI_DELETE.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100_MARCA_DESMARCA_INFO**: Extraer datos desde Oracle filtrando por folio y subproceso
# MAGIC 2. **TF_400_MOD_TIPODATO**: Aplicar transformaciones de mapeo de campos y cálculo de instituto
# MAGIC 3. **DB_500_MARCA_DESMARCA**: Ejecutar DELETE de registros existentes y INSERT de datos transformados

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

# DBTITLE 1,DB_100_MARCA_DESMARCA_INFO - Extracción desde Oracle
# Leer datos desde Oracle y escribir en Delta
statement_001 = query.get_statement(
    "PATRIF_REP_0020_REP_SOL_DESM_001_OCI_EXTRACCION.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_MARCA_DESMARCA_INFO=conf.TL_PRO_MARCA_DESMARCA_INFO,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

db.write_delta(
    f"TEMP_MARCA_DESMARCA_INFO_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite"
)

if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_DESMARCA_INFO_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MARCA_DESMARCA_INFO_{params.sr_folio}"))

logger.info("Datos extraídos desde Oracle y guardados en Delta")

# COMMAND ----------

conf

# COMMAND ----------

# DBTITLE 2,TF_400_MOD_TIPODATO - Transformación de Datos
# Aplicar transformaciones usando SQL desde Delta
statement_002 = query.get_statement(
    "PATRIF_REP_0020_REP_SOL_DESM_002_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)

db.write_delta(
    f"TEMP_MARCA_DESMARCA_TRANSFORM_{params.sr_folio}",
    db.sql_delta(statement_002),
    "overwrite"
)

if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_DESMARCA_TRANSFORM_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MARCA_DESMARCA_TRANSFORM_{params.sr_folio}"))

logger.info("Transformaciones aplicadas exitosamente")

# COMMAND ----------

# DBTITLE 3,DB_500_MARCA_DESMARCA - DELETE e INSERT en Oracle
# Ejecutar DELETE de registros existentes
logger.info("Ejecutando DELETE en la tabla final de Oracle.")
statement_003 = query.get_statement(
    "PATRIF_REP_0020_REP_SOL_DESM_003_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_MARCA_DESMARCA_REP=conf.TL_DTM_MARCA_DESMARCA_REP,
    SR_FOLIO=params.sr_folio
)

db.execute_oci_dml(
    statement=statement_003,
    async_mode=False
)

# Insertar datos transformados directamente en la tabla final de Oracle
logger.info("Insertando datos directamente en la tabla final de Oracle.")
df_resultado = db.read_delta(f"TEMP_MARCA_DESMARCA_TRANSFORM_{params.sr_folio}")
db.write_data(
    df_resultado,
    f"{conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_MARCA_DESMARCA_REP}",
    "default",
    "append"
)

logger.info("INSERT ejecutado exitosamente en Oracle.")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())

