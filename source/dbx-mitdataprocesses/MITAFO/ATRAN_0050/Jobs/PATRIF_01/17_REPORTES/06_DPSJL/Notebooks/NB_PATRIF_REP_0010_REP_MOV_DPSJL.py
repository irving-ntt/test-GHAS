# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_REP_0010_REP_MOV_DPSJL
# MAGIC
# MAGIC **Descripción:** Job que extrae información de la tabla de procesos de devolución de pagos sin justificación legal para los reportes de Devolución de Saldos Sin Justificación Legal.
# MAGIC
# MAGIC **Subetapa:** Extracción de datos base de devolución de pagos
# MAGIC
# MAGIC **Trámite:** Reportes de Devolución de Pagos Sin Justificación Legal
# MAGIC
# MAGIC **Tablas Input:**
# MAGIC - PROCESOS.TTCRXGRAL_DEV_PAG_SJL (Oracle - tabla principal de procesos de devolución de pagos sin justificación legal)
# MAGIC - CIERREN_ETL.TTAFOTRAS_ETL_DEV_PAG_SJL (Oracle - tabla ETL con información de liquidación)
# MAGIC
# MAGIC **Tablas Output:** NA
# MAGIC
# MAGIC **Tablas DELTA:**
# MAGIC - DS_TRN_INFON_#SR_FOLIO#_REPORTES_01
# MAGIC
# MAGIC **Archivos SQL:**
# MAGIC - PATRIF_REP_0010_REP_MOV_DPSJL_001_OCI_DEV_PAG_SJL.sql
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Flujo del Job:
# MAGIC 1. **DB_0100_DEV_PAG_SJL**: Extraer datos desde Oracle con JOIN entre tablas de procesos y ETL
# MAGIC 2. **DS_200_MOV_VIV_RCV**: Almacenar datos en tabla Delta temporal para procesamiento posterior

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
params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()
display(query.get_sql_list())

# COMMAND ----------

# DBTITLE 1,DB_0100_DEV_PAG_SJL - Extracción desde Oracle
# Leer datos desde Oracle con JOIN entre tablas de procesos y ETL
statement_001 = query.get_statement(
    "PATRIF_REP_0010_REP_MOV_DPSJL_001_OCI_DEV_PAG_SJL.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_DEV_PAG_SJL=conf.TL_PRO_DEV_PAG_SJL,
    CX_CRE_ESQUEMA=conf.CX_CRE_ESQUEMA,
    TL_ETL_DPSJL=conf.TL_ETL_DPSJL,
    SR_FOLIO=params.sr_folio,
)

# COMMAND ----------

# DBTITLE 2,DS_200_MOV_VIV_RCV - Almacenar en Delta
# Almacenar datos en tabla Delta temporal para procesamiento posterior
db.write_delta(
    f"DS_TRN_INFON_{params.sr_folio}_REPORTES_01",
    db.read_data("default", statement_001),
    "overwrite"
)
if conf.debug:
    display(db.read_delta(f"DS_TRN_INFON_{params.sr_folio}_REPORTES_01").count())
    display(db.read_delta(f"DS_TRN_INFON_{params.sr_folio}_REPORTES_01"))
logger.info("Datos extraídos desde Oracle y guardados en Delta")

# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
