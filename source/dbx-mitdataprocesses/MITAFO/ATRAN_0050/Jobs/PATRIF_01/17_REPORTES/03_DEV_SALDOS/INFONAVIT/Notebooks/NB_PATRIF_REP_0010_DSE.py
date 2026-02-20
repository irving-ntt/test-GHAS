# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0010_DSE
# MAGIC
# MAGIC **Descripción:** Cargar información en la tabla DATAMARTS.TMAFOTRAS_DEVO_SALDO_EXCE. El notebook lee datos de devolución de saldos excedentes desde la tabla PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO, aplica transformaciones y reglas de negocio, y carga los datos transformados en la tabla de datamart.
# MAGIC
# MAGIC **Subetapa:** 4407
# MAGIC
# MAGIC **Trámite:** 97 - Devolución de Saldos Excedentes
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO (Oracle - Tabla ETL de devolución de saldos excedentes)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_DEVO_SALDO_EXCE (Oracle - Tabla de datamart de devolución de saldos excedentes)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_DEVO_SALDO_EXC_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0010_DSE_001_OCI_READ.sql
# MAGIC - PATRIF_REP_0010_DSE_002_DELTA_TRANSFORM.sql
# MAGIC - PATRIF_REP_0010_DSE_003_OCI_DELETE.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **[DB_0100_TTAFOTRAS_DEVO_SALDO_EXC_INFO]**: Leer información de devolución de saldos excedentes desde la tabla PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO filtrada por folio
# MAGIC 2. **[TF_0200_REGLAS]**: Aplicar transformaciones y reglas de negocio, incluyendo NullToZero para FTN_NUM_CRED_INFONA, conversiones de tipos y asignación de valores constantes
# MAGIC 3. **[DB_0300_TMAFOTRAS_DEVO_SALDO_EXCE]**: Eliminar previamente los registros del mismo folio y luego insertar los datos transformados en la tabla DATAMARTS.TMAFOTRAS_DEVO_SALDO_EXCE

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

# DBTITLE 1,Extracción de datos desde Oracle
statement_001 = query.get_statement(
    "PATRIF_REP_0010_DSE_001_OCI_READ.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_ETL_DEVO_SAL_EXC=conf.TL_ETL_DEVO_SAL_EXC,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_source = db.read_data("default", statement_001)
db.write_delta(
    f"TEMP_DEVO_SALDO_EXC_{params.sr_folio}",
    df_source,
    "overwrite",
)
if conf.debug:
    display(df_source)
logger.info(f"Datos leídos desde Oracle: {df_source.count()} registros")

# COMMAND ----------

# DBTITLE 1,Transformación de datos en Delta
statement_002 = query.get_statement(
    "PATRIF_REP_0010_DSE_002_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO,
    SR_USUARIO=params.sr_usuario,
)

# COMMAND ----------

df_transform = db.sql_delta(statement_002)
if conf.debug:
    display(df_transform)
logger.info(f"Datos transformados: {df_transform.count()} registros")

# COMMAND ----------

# DBTITLE 1,Eliminación previa de registros del mismo folio
statement_003 = query.get_statement(
    "PATRIF_REP_0010_DSE_003_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_DEVO_SAD_EXC=conf.TL_DTM_DEVO_SAD_EXC,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=statement_003, async_mode=False)
logger.info(f"DELETE ejecutado: {execution}")

# COMMAND ----------

# DBTITLE 1,Inserción de datos transformados en Oracle
db.write_data(
    df_transform,
    f"{conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_DEVO_SAD_EXC}",
    "default",
    "append",
)
logger.info(f"Datos insertados en Oracle: {df_transform.count()} registros")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
logger.info("Procesamiento completado exitosamente")
