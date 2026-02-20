# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0020_REP_TRANSFERENCIAS
# MAGIC
# MAGIC **Descripción:** Poblar la tabla de reportes de transferencias INFONAVIT (TMAFOTRAS_TRANSFERENCIAS) en el esquema DATAMARTS. El notebook procesa información de transferencias desde la tabla PROCESOS.TTCRXGRAL_TRANS_INFONA, realiza transformaciones según el tipo de subproceso y genera un dataset para el catálogo de clientes.
# MAGIC
# MAGIC **Subetapa:** 4407
# MAGIC
# MAGIC **Trámite:** 97 - Transferencias INFONAVIT
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_TRANS_INFONA (Oracle - Tabla de transferencias INFONAVIT)
# MAGIC - PROCESOS.TTSISGRAL_SUF_SALDOS (Oracle - Tabla de sufijos de saldos)
# MAGIC - CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS (Oracle - Tabla de movimientos ETL)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_TRANSFERENCIAS (Oracle - Tabla de reportes de transferencias)
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_TRANSFERENCIAS_{sr_folio}
# MAGIC - TEMP_SALDOS_{sr_folio}
# MAGIC - TEMP_JOIN_AIVS_{sr_folio}
# MAGIC - TEMP_MOVIMIENTOS_{sr_folio}
# MAGIC - TEMP_JOIN_MOVIMIENTOS_{sr_folio}
# MAGIC - TEMP_TRANSFORM_{sr_folio}
# MAGIC - DELTA_CLIENTES_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_001_OCI_TRANSFERENCIAS.sql
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_002_OCI_SALDOS.sql
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_003_DELTA_JOIN_AIVS.sql
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_004_OCI_MOVIMIENTOS.sql
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_005_DELTA_JOIN_MOVIMIENTOS.sql
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_006_DELTA_TRANSFORM.sql
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_007_OCI_DELETE.sql
# MAGIC - PATRIF_REP_0020_REP_TRANSFERENCIAS_008_DELTA_CLIENTES.sql
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **[DB_100_PRO_TRANSFERENCIAS]**: Extraer registros de transferencias INFONAVIT desde Oracle con información de saldos asociados
# MAGIC 2. **[DB_200_PRO_SALDOS]**: Extraer el valor AIVS desde la tabla de sufijos de saldos
# MAGIC 3. **[JN_200_VALOR_AIVS]**: Enriquecer los datos de transferencias con el valor AIVS obtenido de la tabla de saldos
# MAGIC 4. **[DB_301_ETL_MOVIMIENTOS]**: Extraer información de movimientos ETL generados para el folio
# MAGIC 5. **[JN_301_MOV]**: Enriquecer los datos de transferencias con la fecha de movimiento desde ETL_MOVIMIENTOS
# MAGIC 6. **[TF_200_MOD_TIPODATO]**: Modificar tipos de datos y dividir los registros según el subproceso (365, 368, 364) para aplicar lógica específica
# MAGIC 7. **[FN_300_SUBPROCESOS]**: Unir las tres salidas del transformer (AG, UG, TAC) en un solo flujo de datos
# MAGIC 8. **[BD_400_DM_TRANSFERENCIAS]**: Insertar los registros transformados en la tabla de reportes DATAMARTS.TMAFOTRAS_TRANSFERENCIAS
# MAGIC 9. **[DS_500_CTES]**: Escribir un dataset con información de clientes para uso posterior en el proceso de catálogo

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
logger.info(f"Iniciando procesamiento para folio: {params.sr_folio}, subproceso: {params.sr_subproceso}")

# COMMAND ----------

# DBTITLE 1,Extracción de transferencias desde Oracle
statement_001 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_001_OCI_TRANSFERENCIAS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_TRANS_INFONA=conf.TL_PRO_TRANS_INFONA,
    TL_PRO_SUF_SALDOS=conf.TL_PRO_SUF_SALDOS,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

# COMMAND ----------

df_transferencias = db.read_data("default", statement_001)
db.write_delta(
    f"TEMP_TRANSFERENCIAS_{params.sr_folio}",
    df_transferencias,
    "overwrite",
)
if conf.debug:
    display(df_transferencias)
logger.info(f"Transferencias extraídas: {df_transferencias.count()} registros")

# COMMAND ----------

# DBTITLE 1,Extracción de saldos AIVS desde Oracle
statement_002 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_002_OCI_SALDOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_SUF_SALDOS=conf.TL_PRO_SUF_SALDOS,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

# COMMAND ----------

df_saldos = db.read_data("default", statement_002)
db.write_delta(
    f"TEMP_SALDOS_{params.sr_folio}",
    df_saldos,
    "overwrite",
)
if conf.debug:
    display(df_saldos)
logger.info(f"Saldos AIVS extraídos: {df_saldos.count()} registros")

# COMMAND ----------

# DBTITLE 1,Join de transferencias con saldos AIVS
statement_003 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_003_DELTA_JOIN_AIVS.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_join_aivs = db.sql_delta(statement_003)
db.write_delta(
    f"TEMP_JOIN_AIVS_{params.sr_folio}",
    df_join_aivs,
    "overwrite",
)
if conf.debug:
    display(df_join_aivs)
logger.info(f"Join AIVS completado: {df_join_aivs.count()} registros")

# COMMAND ----------

# DBTITLE 1,Extracción de movimientos ETL desde Oracle
statement_004 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_004_OCI_MOVIMIENTOS.sql",
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_ETL_MOVIMIENTOS=conf.TL_ETL_MOVIMIENTOS,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_movimientos = db.read_data("default", statement_004)
db.write_delta(
    f"TEMP_MOVIMIENTOS_{params.sr_folio}",
    df_movimientos,
    "overwrite",
)
if conf.debug:
    display(df_movimientos)
logger.info(f"Movimientos ETL extraídos: {df_movimientos.count()} registros")

# COMMAND ----------

# DBTITLE 1,Join de transferencias con movimientos ETL
statement_005 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_005_DELTA_JOIN_MOVIMIENTOS.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_join_movimientos = db.sql_delta(statement_005)
db.write_delta(
    f"TEMP_JOIN_MOVIMIENTOS_{params.sr_folio}",
    df_join_movimientos,
    "overwrite",
)
if conf.debug:
    display(df_join_movimientos)
logger.info(f"Join movimientos completado: {df_join_movimientos.count()} registros")

# COMMAND ----------

# DBTITLE 1,Transformación de datos según subproceso
statement_006 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_006_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_transform = db.sql_delta(statement_006)
db.write_delta(
    f"TEMP_TRANSFORM_{params.sr_folio}",
    df_transform,
    "overwrite",
)
if conf.debug:
    display(df_transform)
logger.info(f"Transformación completada: {df_transform.count()} registros")

# COMMAND ----------

# DBTITLE 1,Eliminación de registros previos del folio
statement_007 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_007_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_TRANSFERENCIAS=conf.TL_DTM_TRANSFERENCIAS,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)

# COMMAND ----------

execution = db.execute_oci_dml(statement=statement_007, async_mode=False)
logger.info(f"DELETE ejecutado: {execution}")

# COMMAND ----------

# DBTITLE 1,Inserción de registros transformados en tabla de reportes
db.write_data(
    df_transform,
    f"{conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_TRANSFERENCIAS}",
    "default",
    "append",
)
logger.info(f"INSERT completado: {df_transform.count()} registros insertados")

# COMMAND ----------

# DBTITLE 1,Generación de dataset de clientes
statement_008 = query.get_statement(
    "PATRIF_REP_0020_REP_TRANSFERENCIAS_008_DELTA_CLIENTES.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

df_clientes = db.sql_delta(statement_008)
db.write_delta(
    f"DELTA_CLIENTES_{params.sr_folio}",
    df_clientes,
    "overwrite",
)
if conf.debug:
    display(df_clientes)
logger.info(f"Dataset de clientes generado: {df_clientes.count()} registros")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
logger.info("Procesamiento completado exitosamente")
