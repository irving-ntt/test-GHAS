# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0010_DSEF
# MAGIC
# MAGIC **Descripción:** Job que carga información en la tabla de DATAMARTS.TMAFOTRAS_DEVO_SALDO_EXCE para Reportes.
# MAGIC
# MAGIC **Subetapa:** Reportes
# MAGIC
# MAGIC **Trámite:** 0010 - Devolución de Saldo Excedente FOVISSSTE
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_FOV (Oracle - Tabla de procesos de devolución de saldo excedente FOVISSSTE)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_DEV_SALDO_EXCE_FOVI (Oracle - Tabla de datamarts de devolución de saldo excedente FOVISSSTE)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_PRO_DEV_SLD_ECX_FOV_{sr_folio}
# MAGIC - TEMP_PRO_DEV_SLD_ECX_FOV_TRANSFORM_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0010_DSEF_001_OCI_EXTRACCION.sql
# MAGIC - PATRIF_REP_0010_DSEF_002_DELTA_TRANSFORM.sql
# MAGIC - PATRIF_REP_0010_DSEF_003_OCI_DELETE.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_0100_TTAFOTRAS_DEVO_SALDO_EXC_FOV**: Extracción de datos desde Oracle filtrados por folio
# MAGIC 2. **TF_0200_REGLAS**: Transformación y mapeo de campos en Delta
# MAGIC 3. **DB_0300_TMAFOTRAS_DEVO_SALDO_EXCE**: Eliminación de registros existentes y carga de datos transformados a Oracle

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

# DBTITLE 1,EXTRACCIÓN DE ORACLE
logger.info(f"Iniciando procesamiento para folio: {params.sr_folio}")

# Paso 1: Extracción desde Oracle
logger.info("Paso 1: Extrayendo datos desde Oracle")
statement = query.get_statement(
    "PATRIF_REP_0010_DSEF_001_OCI_EXTRACCION.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_DEV_SLD_ECX_FOV=conf.TL_PRO_DEV_SLD_ECX_FOV,
    SR_FOLIO=params.sr_folio
)
temp_table_extract = f"TEMP_PRO_DEV_SLD_ECX_FOV_{params.sr_folio}"

db.write_delta(
    temp_table_extract,
    db.read_data("default", statement),
    "overwrite"
)

if conf.debug:
    display(db.read_delta(temp_table_extract).count())
    display(db.read_delta(temp_table_extract))

logger.info(f"Datos extraídos y guardados en tabla Delta: {temp_table_extract}")

# COMMAND ----------

# DBTITLE 2,APLICACIÓN DE LÓGICA CON SQL
# Paso 2: Aplicar transformaciones en Delta
logger.info("Paso 2: Aplicando transformaciones en Delta")
statement_transform = query.get_statement(
    "PATRIF_REP_0010_DSEF_002_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO
)
temp_table_transform = f"TEMP_PRO_DEV_SLD_ECX_FOV_TRANSFORM_{params.sr_folio}"

db.write_delta(
    temp_table_transform,
    db.sql_delta(statement_transform),
    "overwrite"
)

if conf.debug:
    display(db.read_delta(temp_table_transform).count())
    display(db.read_delta(temp_table_transform))

logger.info(f"Datos transformados guardados en tabla Delta: {temp_table_transform}")

# COMMAND ----------

# DBTITLE 3,CARGA A ORACLE
# Paso 3: Ejecutar DELETE antes del INSERT
logger.info("Paso 3: Ejecutando DELETE de registros existentes")
statement_delete = query.get_statement(
    "PATRIF_REP_0010_DSEF_003_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_DEV_SLD_ECX_FOV=conf.TL_DTM_DEV_SLD_ECX_FOV,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */"
)
db.execute_oci_dml(statement=statement_delete, async_mode=False)
logger.info("DELETE ejecutado correctamente")

# COMMAND ----------

# Paso 4: Cargar datos transformados a Oracle
logger.info("Paso 4: Cargando datos transformados a Oracle")
target_table = f"{conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_DEV_SLD_ECX_FOV}"
db.write_data(
    db.read_delta(temp_table_transform),
    target_table,
    "default",
    "append"
)
logger.info(f"Datos cargados correctamente en {target_table}")

# COMMAND ----------

# Limpieza de tablas temporales
logger.info("Limpiando tablas temporales")
CleanUpManager.cleanup_notebook(locals())
logger.info("Proceso completado exitosamente")
