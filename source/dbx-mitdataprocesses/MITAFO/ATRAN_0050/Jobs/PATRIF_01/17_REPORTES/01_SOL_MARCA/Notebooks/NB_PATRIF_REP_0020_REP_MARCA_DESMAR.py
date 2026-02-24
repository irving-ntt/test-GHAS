# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0020_REP_MARCA_DESMAR
# MAGIC
# MAGIC **Descripción:** Job paralelo que lee datos de marca/desmarca desde dos tablas Oracle, realiza un LEFT OUTER JOIN, aplica transformaciones y elimina/inserta registros en la tabla de marca/desmarca en Oracle.
# MAGIC
# MAGIC **Subetapa:** Procesamiento de marca/desmarca INFONAVIT-FOVISSSTE
# MAGIC
# MAGIC **Trámite:** Reportes de marca/desmarca INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO (Oracle - información de marca/desmarca INFONAVIT-FOVISSSTE)
# MAGIC - PROCESOS.TTSISGRAL_SUF_SALDOS (Oracle - saldos de subcuentas)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_MARCA_DESMARCA (Oracle - tabla de marca/desmarca)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_MARCA_DESMAR_RAW_{sr_folio}
# MAGIC - TEMP_MARCA_DESMAR_SUF_SALDOS_{sr_folio}
# MAGIC - TEMP_MARCA_DESMAR_JOINED_{sr_folio}
# MAGIC - TEMP_MARCA_DESMAR_TRANSFORMED_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0020_REP_MARCA_DESMAR_001_OCI_READ_MARCA_DESMARCA_INFO.sql
# MAGIC - PATRIF_REP_0020_REP_MARCA_DESMAR_002_OCI_READ_SUF_SALDOS.sql
# MAGIC - PATRIF_REP_0020_REP_MARCA_DESMAR_003_DELTA_JOIN.sql
# MAGIC - PATRIF_REP_0020_REP_MARCA_DESMAR_004_DELTA_TRANSFORM.sql
# MAGIC - PATRIF_REP_0020_REP_MARCA_DESMAR_005_OCI_DELETE.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_100_MARCA_DESMARCA_INFO**: Extraer datos desde Oracle (PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO)
# MAGIC 2. **DB_100_SUF_SALDOS**: Extraer datos agregados desde Oracle (PROCESOS.TTSISGRAL_SUF_SALDOS)
# MAGIC 3. **JO_300_SUF_SALDOS**: Realizar LEFT OUTER JOIN entre ambas fuentes
# MAGIC 4. **TF_400_MOD_TIPODATO**: Aplicar transformaciones (mapeo de campos y lógica condicional)
# MAGIC 5. **DB_500_MARCA_DESMARCA**: DELETE e INSERT en Oracle (DATAMARTS.TMAFOTRAS_MARCA_DESMARCA)

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
logger.info(f"Iniciando extracción de datos de marca/desmarca para folio: {params.sr_folio}")
statement_oci_read_marca = query.get_statement(
    "PATRIF_REP_0020_REP_MARCA_DESMAR_001_OCI_READ_MARCA_DESMARCA_INFO.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_MARCA_DESMARCA_INFO=conf.TL_PRO_MARCA_DESMARCA_INFO,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)
df_marca_desmarca = db.read_data("default", statement_oci_read_marca)
db.write_delta(
    f"TEMP_MARCA_DESMAR_RAW_{params.sr_folio}",
    df_marca_desmarca,
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_DESMAR_RAW_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MARCA_DESMAR_RAW_{params.sr_folio}"))
logger.info("Datos de marca/desmarca extraídos desde Oracle y guardados en Delta.")

# COMMAND ----------

# DBTITLE 2,DB_100_SUF_SALDOS - Extracción de saldos desde Oracle
logger.info("Iniciando extracción de saldos de subcuentas desde Oracle.")
statement_oci_read_saldos = query.get_statement(
    "PATRIF_REP_0020_REP_MARCA_DESMAR_002_OCI_READ_SUF_SALDOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_SUF_SALDOS=conf.TL_PRO_SUF_SALDOS,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)
df_suf_saldos = db.read_data("default", statement_oci_read_saldos)
db.write_delta(
    f"TEMP_MARCA_DESMAR_SUF_SALDOS_{params.sr_folio}",
    df_suf_saldos,
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_DESMAR_SUF_SALDOS_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MARCA_DESMAR_SUF_SALDOS_{params.sr_folio}"))
logger.info("Datos de saldos de subcuentas extraídos desde Oracle y guardados en Delta.")

# COMMAND ----------

# DBTITLE 3,JO_300_SUF_SALDOS - LEFT OUTER JOIN
logger.info("Realizando LEFT OUTER JOIN entre marca/desmarca y saldos.")
statement_delta_join = query.get_statement(
    "PATRIF_REP_0020_REP_MARCA_DESMAR_003_DELTA_JOIN.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)
df_joined = db.sql_delta(statement_delta_join)
db.write_delta(
    f"TEMP_MARCA_DESMAR_JOINED_{params.sr_folio}",
    df_joined,
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_DESMAR_JOINED_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MARCA_DESMAR_JOINED_{params.sr_folio}"))
logger.info("JOIN realizado exitosamente y guardado en Delta.")

# COMMAND ----------

# DBTITLE 4,TF_400_MOD_TIPODATO - Aplicar Transformaciones
logger.info("Aplicando transformaciones del transformer.")
statement_delta_transform = query.get_statement(
    "PATRIF_REP_0020_REP_MARCA_DESMAR_004_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio
)
df_transformed = db.sql_delta(statement_delta_transform)
db.write_delta(
    f"TEMP_MARCA_DESMAR_TRANSFORMED_{params.sr_folio}",
    df_transformed,
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_MARCA_DESMAR_TRANSFORMED_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MARCA_DESMAR_TRANSFORMED_{params.sr_folio}"))
logger.info("Transformaciones aplicadas exitosamente y guardadas en Delta.")

# COMMAND ----------

# DBTITLE 5,DB_500_MARCA_DESMARCA - DELETE e INSERT en Oracle
statement_oci_delete = query.get_statement(
    "PATRIF_REP_0020_REP_MARCA_DESMAR_005_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_PRO_MARCA_DESMARCA_REP=conf.TL_PRO_MARCA_DESMARCA_REP,
    SR_FOLIO=params.sr_folio
)
execution_delete = db.execute_oci_dml(
    statement=statement_oci_delete,
    async_mode=False
)

df_final_for_insert = db.read_delta(f"TEMP_MARCA_DESMAR_TRANSFORMED_{params.sr_folio}")
db.write_data(
    df_final_for_insert,
    f"{conf.CX_DTM_ESQUEMA}.{conf.TL_PRO_MARCA_DESMARCA_REP}",
    "default",
    "append"
)
if conf.debug:
    count_inserted = db.read_data("default", f"SELECT COUNT(*) AS CNT FROM {conf.CX_DTM_ESQUEMA}.{conf.TL_PRO_MARCA_DESMARCA_REP} WHERE FTC_FOLIO_BITA = '{params.sr_folio}'")
    display(count_inserted)
logger.info("INSERT ejecutado exitosamente en Oracle.")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
