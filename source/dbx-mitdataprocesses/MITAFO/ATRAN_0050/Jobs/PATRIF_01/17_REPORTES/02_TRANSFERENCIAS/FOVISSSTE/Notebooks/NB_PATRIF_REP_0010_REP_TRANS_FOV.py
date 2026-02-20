# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0010_REP_TRANS_FOV
# MAGIC
# MAGIC **Descripción:** Job que pobla la tabla de catálogo de clientes BDNSAR para reportes de Cognos. Extrae información de clientes desde la tabla de transferencias FOVISSSTE y realiza operación Update/Insert basada en número de cuenta individual.
# MAGIC
# MAGIC **Subetapa:** REPORTES
# MAGIC
# MAGIC **Trámite:** Transferencias FOVISSSTE
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_TRANS_FOVISSSTE (Oracle - Tabla de transferencias FOVISSSTE)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TDAFOTRAS_CAT_CTE_BDNSAR (Oracle - Catálogo de clientes BDNSAR)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_PROCESOS_{sr_folio}
# MAGIC - TEMP_MAPEO_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0010_REP_TRANS_FOV_001_OCI_PROCESOS.sql
# MAGIC - PATRIF_REP_0010_REP_TRANS_FOV_002_DELTA_MAPEO.sql
# MAGIC - PATRIF_REP_0010_REP_TRANS_FOV_003_OCI_MERGE.sql
# MAGIC - PATRIF_REP_0010_REP_TRANS_FOV_004_OCI_DELETE_AUX.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_0100_TL_PRO_FOV**: Extraer información de clientes desde Oracle filtrando por folio y estatus activo
# MAGIC 2. **TF_0200_MAPEO**: Mapear campos desde origen hacia formato destino con renombrados
# MAGIC 3. **DB_0300_TL_CAT_CTE_BDNSAR**: Cargar datos en tabla auxiliar y ejecutar MERGE Update/Insert en tabla destino
# MAGIC 4. **Limpieza**: Eliminar datos de tabla auxiliar después del MERGE

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

# DBTITLE 1,Extracción desde Oracle
statement_001 = query.get_statement(
    "PATRIF_REP_0010_REP_TRANS_FOV_001_OCI_PROCESOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_TRANS_FOV=conf.TL_PRO_TRANS_FOV,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_oracle = db.read_data("default", statement_001)
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
statement_002 = query.get_statement(
    "PATRIF_REP_0010_REP_TRANS_FOV_002_DELTA_MAPEO.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_mapeo = db.sql_delta(statement_002)
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

# DBTITLE 1,Cargar datos en tabla auxiliar Oracle
db.write_data(
    df_mapeo,
    f"{conf.CX_DATAUX_ESQUEMA}.{conf.TL_DTM_CAT_CLIENTE_AUX}",
    "default",
    "append",
)
logger.info(f"Datos cargados en tabla auxiliar: {conf.CX_DATAUX_ESQUEMA}.{conf.TL_DTM_CAT_CLIENTE_AUX}")

# COMMAND ----------

# DBTITLE 1,MERGE en tabla de catálogo de clientes
statement_003 = query.get_statement(
    "PATRIF_REP_0010_REP_TRANS_FOV_003_OCI_MERGE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_CAT_CLIENTE=conf.TL_DTM_CAT_CLIENTE,
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CLIENTE_AUX=conf.TL_DTM_CAT_CLIENTE_AUX,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=statement_003, async_mode=False)
logger.info(f"MERGE ejecutado: {execution}")

# COMMAND ----------

# DBTITLE 1,Limpieza de tabla auxiliar
statement_004 = query.get_statement(
    "PATRIF_REP_0010_REP_TRANS_FOV_004_OCI_DELETE_AUX.sql",
    CX_DATAUX_ESQUEMA=conf.CX_DATAUX_ESQUEMA,
    TL_DTM_CAT_CLIENTE_AUX=conf.TL_DTM_CAT_CLIENTE_AUX,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=statement_004, async_mode=False)
logger.info(f"Limpieza de tabla auxiliar ejecutada: {execution}")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
