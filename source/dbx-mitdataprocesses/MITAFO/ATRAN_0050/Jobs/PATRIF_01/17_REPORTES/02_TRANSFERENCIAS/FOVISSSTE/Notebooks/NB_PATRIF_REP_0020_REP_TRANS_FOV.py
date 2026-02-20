# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0020_REP_TRANS_FOV
# MAGIC
# MAGIC **Descripción:** Job que pobla la tabla de reportes de transferencias FOVISSSTE para reportes de Cognos. Extrae información completa de transferencias desde la tabla de origen, aplica mapeo de campos y realiza operación INSERT previa limpieza de registros existentes del mismo folio.
# MAGIC
# MAGIC **Subetapa:** REPORTES
# MAGIC
# MAGIC **Trámite:** Transferencias FOVISSSTE
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_TRANS_FOVISSSTE (Oracle - Tabla de transferencias FOVISSSTE)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_TRANS_FOVI (Oracle - Tabla de reportes de transferencias FOVISSSTE)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_PROCESOS_{sr_folio}
# MAGIC - TEMP_MAPEO_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0020_REP_TRANS_FOV_001_OCI_DELETE.sql
# MAGIC - PATRIF_REP_0020_REP_TRANS_FOV_002_OCI_PROCESOS.sql
# MAGIC - PATRIF_REP_0020_REP_TRANS_FOV_003_DELTA_MAPEO.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DELETE previo**: Eliminar registros existentes del mismo folio en tabla destino
# MAGIC 2. **DB_0100_TL_PRO_FOV**: Extraer información completa de transferencias desde Oracle filtrando por folio
# MAGIC 3. **TF_0200_MAPEO**: Mapear campos desde origen hacia formato destino con múltiples renombrados
# MAGIC 4. **DB_0300_TL_DMT_TRANSFOV**: Insertar datos directamente en tabla destino

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

# COMMAND ----------

# DBTITLE 1,DELETE previo de registros existentes
statement_001 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANS_FOV_001_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_TRANSF_FOV=conf.TL_DTM_TRANSF_FOV,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=statement_001, async_mode=False)
logger.info(f"DELETE previo ejecutado: {execution}")

# COMMAND ----------

# DBTITLE 1,Extracción desde Oracle
statement_002 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANS_FOV_002_OCI_PROCESOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_TRANS_FOV=conf.TL_PRO_TRANS_FOV,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_oracle = db.read_data("default", statement_002)
logger.info(f"Registros extraídos desde Oracle: {df_oracle.count()}")

# COMMAND ----------

# DBTITLE 1,Guardar en Delta temporal
db.write_delta(
    f"TEMP_PROCESOS_{params.sr_folio}",
    df_oracle,
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"TEMP_PROCESOS_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Mapeo de campos en Delta
statement_003 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANS_FOV_003_DELTA_MAPEO.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_mapeo = db.sql_delta(statement_003)
df_mapeo.cache()
if conf.debug:
    display(df_mapeo)

# COMMAND ----------

# DBTITLE 1,Guardar resultado del mapeo en Delta
db.write_delta(
    f"TEMP_MAPEO_{params.sr_folio}",
    df_mapeo,
    "overwrite",
)
if conf.debug:
    display(db.read_delta(f"TEMP_MAPEO_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,INSERT directo en tabla de reportes de transferencias
db.write_data(
    df_mapeo,
    f"{conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_TRANSF_FOV}",
    "default",
    "append",
)
logger.info(f"Datos insertados en tabla destino: {conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_TRANSF_FOV}")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
