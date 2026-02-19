# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0020_CAT_CLIENTE
# MAGIC
# MAGIC **Descripción:** Poblar la tabla de catálogo de clientes INFONAVIT (DATAMARTS.TDAFOTRAS_CAT_CTE_INFONAVIT) tomando como base lo procesado en DEVOLUCION DE SALDOS EXCEDENTES. El notebook lee información de clientes desde la tabla PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO, aplica un mapeo de campos y realiza un UPDATE/INSERT (upsert) en la tabla de catálogo usando el NSS como clave.
# MAGIC
# MAGIC **Subetapa:** 4407
# MAGIC
# MAGIC **Trámite:** 97 - Devolución de Saldos Excedentes
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO (Oracle - Tabla ETL de devolución de saldos excedentes)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TDAFOTRAS_CAT_CTE_INFONAVIT (Oracle - Tabla de catálogo de clientes INFONAVIT)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_CLIENTES_{sr_folio}
# MAGIC
# MAGIC **Tablas Auxiliares:**
# MAGIC - CIERREN_DATAUX.TDAFOTRAS_CAT_CTE_INFONAVIT_AUX (Oracle - Tabla auxiliar para MERGE)
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0020_CAT_CLIENTE_001_OCI_READ.sql
# MAGIC - PATRIF_REP_0020_CAT_CLIENTE_002_DELTA_MAPEO.sql
# MAGIC - PATRIF_REP_0020_CAT_CLIENTE_003_OCI_MERGE.sql
# MAGIC - PATRIF_REP_0020_CAT_CLIENTE_004_OCI_DELETE_AUX.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **[DB_100_TTAFOTRAS_DEVO_SALDO_EXC_INFO]**: Leer información de clientes desde la tabla PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO filtrada por folio, seleccionando solo los campos necesarios para el catálogo
# MAGIC 2. **[TF_200_MAPEO]**: Mapear los campos de entrada a los campos requeridos por la tabla de catálogo de clientes INFONAVIT
# MAGIC 3. **[Carga en tabla auxiliar]**: Cargar datos mapeados en tabla auxiliar CIERREN_DATAUX.TDAFOTRAS_CAT_CTE_INFONAVIT_AUX usando `db.write_data()` con modo `"append"`
# MAGIC 4. **[BD_300_TDCRXGRAL_CAT_CLIENTE]**: Realizar UPDATE/INSERT (upsert) en la tabla de catálogo de clientes INFONAVIT usando FTC_NSS_INFONA como clave primaria mediante MERGE desde la tabla auxiliar
# MAGIC 5. **[Limpieza tabla auxiliar]**: Eliminar todos los registros de la tabla auxiliar después del MERGE exitoso usando DELETE

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

# DBTITLE 1,Extracción de datos de clientes desde Oracle
statement_001 = query.get_statement(
    "PATRIF_REP_0020_CAT_CLIENTE_001_OCI_READ.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_ETL_DEVO_SALD_EXC=conf.TL_ETL_DEVO_SALD_EXC,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_clientes = db.read_data("default", statement_001)
db.write_delta(
    f"TEMP_CLIENTES_{params.sr_folio}",
    df_clientes,
    "overwrite",
)
if conf.debug:
    display(df_clientes)
logger.info(f"Clientes leídos desde Oracle: {df_clientes.count()} registros")

# COMMAND ----------

# DBTITLE 1,Mapeo de campos para catálogo de clientes
statement_002 = query.get_statement(
    "PATRIF_REP_0020_CAT_CLIENTE_002_DELTA_MAPEO.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_mapeo = db.sql_delta(statement_002)
if conf.debug:
    display(df_mapeo)
logger.info(f"Datos mapeados: {df_mapeo.count()} registros")

# COMMAND ----------

# DBTITLE 1,Carga de datos en tabla auxiliar
db.write_data(
    df_mapeo,
    f"{conf.CX_DATAUX_ESQUEMA}.{conf.TL_DTM_CAT_CTE_INFONAVIT_AUX}",
    "default",
    "append",
)
logger.info(f"Datos cargados en tabla auxiliar: {df_mapeo.count()} registros")

# COMMAND ----------

# DBTITLE 1,MERGE en tabla de catálogo de clientes
statement_003 = query.get_statement(
    "PATRIF_REP_0020_CAT_CLIENTE_003_OCI_MERGE.sql",
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
    "PATRIF_REP_0020_CAT_CLIENTE_004_OCI_DELETE_AUX.sql",
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
