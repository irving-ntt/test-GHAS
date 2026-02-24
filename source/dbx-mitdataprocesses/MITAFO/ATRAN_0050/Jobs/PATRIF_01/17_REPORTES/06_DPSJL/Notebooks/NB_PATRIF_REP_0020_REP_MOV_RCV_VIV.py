# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0020_REP_MOV_RCV_VIV
# MAGIC
# MAGIC **Descripción:** Job que extrae la información de saldos aplicados para devolución de Pagos Sin Justificación Legal desde las tablas de movimientos RCV y VIV, aplicando transformaciones condicionales basadas en el concepto de movimiento para generar los importes aplicables por tipo de concepto.
# MAGIC
# MAGIC **Subetapa:** Extracción de saldos aplicados por concepto de movimiento
# MAGIC
# MAGIC **Trámite:** Reportes de Devolución de Pagos Sin Justificación Legal
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - CIERREN.TTAFOGRAL_MOV_RCV (Oracle - tabla de movimientos RCV)
# MAGIC - CIERREN.TTAFOGRAL_MOV_VIV (Oracle - tabla de movimientos VIV)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - TEMP_MOV_RCV_VIV_#SR_FOLIO#
# MAGIC - DS_TRN_INFON_#SR_FOLIO#_REPORTES_02
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0020_REP_MOV_RCV_VIV_001_OCI_MOV_RCV_VIV.sql
# MAGIC - PATRIF_REP_0020_REP_MOV_RCV_VIV_002_DELTA_TRANSFORM.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_0100_MOV_VIV_RCV**: Extraer datos desde Oracle con UNION de tablas RCV y VIV
# MAGIC 2. **TF_200_SALDOS**: Aplicar transformaciones condicionales para separar importes por concepto
# MAGIC 3. **DS_300_DEV_PAG_SJL**: Almacenar datos en tabla Delta temporal para procesamiento posterior

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

# DBTITLE 1,DB_0100_MOV_VIV_RCV - Extracción desde Oracle
# Leer datos desde Oracle con UNION de tablas RCV y VIV
statement_001 = query.get_statement(
    "PATRIF_REP_0020_REP_MOV_RCV_VIV_001_OCI_MOV_RCV_VIV.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_MOV_RCV=conf.TL_CRN_MOV_RCV,
    TL_CRN_MOV_VIV=conf.TL_CRN_MOV_VIV,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# Escribir datos en Delta temporal
db.write_delta(
    f"TEMP_MOV_RCV_VIV_{params.sr_folio}",
    db.read_data("default", statement_001),
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"TEMP_MOV_RCV_VIV_{params.sr_folio}").count())
    display(db.read_delta(f"TEMP_MOV_RCV_VIV_{params.sr_folio}"))
logger.info("Datos extraídos desde Oracle y guardados en Delta")

# COMMAND ----------

# DBTITLE 2,TF_200_SALDOS - Transformaciones condicionales
# Aplicar transformaciones condicionales para separar importes por concepto de movimiento
statement_002 = query.get_statement(
    "PATRIF_REP_0020_REP_MOV_RCV_VIV_002_DELTA_TRANSFORM.sql",
    CATALOG_SCHEMA=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}",
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 3,DS_300_DEV_PAG_SJL - Almacenar en Delta
# Almacenar datos transformados en tabla Delta temporal para procesamiento posterior
db.write_delta(
    f"DS_TRN_INFON_{params.sr_folio}_REPORTES_02",
    db.sql_delta(statement_002),
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"DS_TRN_INFON_{params.sr_folio}_REPORTES_02").count())
    display(db.read_delta(f"DS_TRN_INFON_{params.sr_folio}_REPORTES_02"))
logger.info("Datos transformados y guardados en Delta")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
