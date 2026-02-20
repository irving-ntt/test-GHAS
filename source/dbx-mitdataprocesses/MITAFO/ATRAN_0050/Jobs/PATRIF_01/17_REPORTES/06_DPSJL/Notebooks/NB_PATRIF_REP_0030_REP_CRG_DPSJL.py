# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0030_REP_CRG_DPSJL
# MAGIC
# MAGIC **Descripción:** Job que carga la tabla de reportes para devolución de pagos sin justificación legal. Combina los datos base de devolución de pagos con los saldos aplicados por concepto de movimiento mediante un LEFT OUTER JOIN, calcula los totales solicitados y aplicables, agrega campos de auditoría, y finalmente inserta los resultados en la tabla de reportes DATAMARTS.TMAFOTRAS_DEV_PAG_SJL.
# MAGIC
# MAGIC **Subetapa:** Carga de reportes de devolución de pagos sin justificación legal
# MAGIC
# MAGIC **Trámite:** Reportes de Devolución de Pagos Sin Justificación Legal
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - DS_TRN_INFON_#SR_FOLIO#_REPORTES_01 (Delta - datos base de devolución de pagos del job 0010)
# MAGIC - DS_TRN_INFON_#SR_FOLIO#_REPORTES_02 (Delta - saldos aplicados del job 0020)
# MAGIC
# MAGIC **Tablas Output:**
# MAGIC - DATAMARTS.TMAFOTRAS_DEV_PAG_SJL (Oracle - tabla de reportes de devolución de pagos sin justificación legal)
# MAGIC
# MAGIC **Tablas DELTA:** NA
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0030_REP_CRG_DPSJL_001_OCI_DELETE.sql
# MAGIC - PATRIF_REP_0030_REP_CRG_DPSJL_002_DELTA_JOIN_TRANSFORM.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DS_200_MOV_VIV_RCV**: Leer datos base desde Delta (job 0010)
# MAGIC 2. **DS_300_DEV_PAG_SJL**: Leer saldos aplicados desde Delta (job 0020)
# MAGIC 3. **JO_300_INFOSALDOS**: LEFT OUTER JOIN entre datos base y saldos aplicados
# MAGIC 4. **TF_400_COMPLETA_INFO**: Calcular totales y agregar campos de auditoría
# MAGIC 5. **DB_0500_DTM_DEV_PAG_SJL**: DELETE previo e INSERT en tabla de reportes Oracle

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
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,DB_0500_DTM_DEV_PAG_SJL - DELETE previo
# Ejecutar DELETE previo en la tabla de reportes
statement_delete = query.get_statement(
    "PATRIF_REP_0030_REP_CRG_DPSJL_001_OCI_DELETE.sql",
    CX_DTM_ESQUEMA=conf.CX_DTM_ESQUEMA,
    TL_DTM_DEV_PAG_SJL=conf.TL_DTM_DEV_PAG_SJL,
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution_delete = db.execute_oci_dml(statement=statement_delete, async_mode=False)

# COMMAND ----------

# DBTITLE 2,JO_300_INFOSALDOS + TF_400_COMPLETA_INFO - JOIN y transformaciones
# Combinar datos base con saldos aplicados y calcular totales
statement_transform = query.get_statement(
    "PATRIF_REP_0030_REP_CRG_DPSJL_002_DELTA_JOIN_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
    CX_CRE_USUARIO=conf.CX_CRE_USUARIO,
)

# COMMAND ----------

# DBTITLE 3,DB_0500_DTM_DEV_PAG_SJL - INSERT en Oracle
# Insertar datos completos en la tabla de reportes
df_resultado = db.sql_delta(statement_transform)
if conf.debug:
    display(df_resultado.count())
    display(df_resultado)
db.write_data(
    df_resultado,
    f"{conf.CX_DTM_ESQUEMA}.{conf.TL_DTM_DEV_PAG_SJL}",
    "default",
    "append"
)
logger.info("Datos insertados exitosamente en la tabla de reportes")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
