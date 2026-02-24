# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PANCIN_MOV_0035_BALANCE
# MAGIC
# MAGIC **Descripción:** Insertar en Oracle `TTAFOGRAL_BALANCE_MOVS` a partir de la Delta `RESULTADO_MOVS_BALANCE_{sr_folio}`
# MAGIC
# MAGIC **Subetapa:** Inserción de balance (OCI)
# MAGIC
# MAGIC **Trámite:** 8 - DO IMSS
# MAGIC
# MAGIC **Tablas Input:** RESULTADO_MOVS_BALANCE_{sr_folio} (Delta)
# MAGIC
# MAGIC **Tablas Output:** {CX_CRN_ESQUEMA}.{TL_CRN_BALANCE_MOVS}
# MAGIC
# MAGIC **Tablas DELTA:** RESULTADO_MOVS_BALANCE_{sr_folio}
# MAGIC
# MAGIC **Archivos SQL:** NA
# MAGIC
# MAGIC **Flow:** Leer Delta → Insertar en Oracle con `write_data(..., append)`
# MAGIC
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/1359597267196344?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/02_ACREDITACION_UNIFICADA/Notebooks/assets/NB_PANCIN_MOV_0035_BALANCE.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,DEFINICIÓN DE PARÁMETROS
params = WidgetParams(
    {
        "sr_paso": str,
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_id_snapshot": str,
        "sr_folio": str,
        "sr_fec_liq": (str, "YYYYMMDD"),
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_fec_acc": (str, "YYYYMMDD"),
        "sr_tipo_mov": str,
        "sr_subetapa": str,
        #"sr_id_archivo": str,
        "sr_reproceso": str,
        "sr_etapa_bit": str,
    }
)

params.validate()
conf = ConfManager()
query = QueryManager()
db = DBXConnectionManager()

logger.info("=== INICIANDO: NB_PANCIN_MOV_0035_BALANCE ===")
logger.info(f"Parámetros: {params}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reglas aplicadas
# MAGIC - Uso de `ds_catalog.txt`: `p_CLV_SUFIJO_04 = RESULTADO_MOVS_BALANCE` → se lee `RESULTADO_MOVS_BALANCE_{sr_folio}`.
# MAGIC - Evitar DataFrames innecesarios: pasar `db.read_delta(...)` directamente a `write_data()`.
# MAGIC - Inserción a Oracle con `write_data(df, "{CX_CRN_ESQUEMA}.{TL_CRN_BALANCE_MOVS}", "default", "append")`.
# MAGIC - Logging con `logger.info()` y limpieza al final.

# COMMAND ----------

# DBTITLE 2,INSERCIÓN EN ORACLE DESDE DELTA
logger.info(
    f"Insertando en {conf.CX_CRN_ESQUEMA}.{conf.TL_CRN_BALANCE_MOVS} desde RESULTADO_MOVS_BALANCE_{params.sr_folio}"
)
db.write_data(
    db.read_delta(f"RESULTADO_MOVS_BALANCE_{params.sr_folio}"),
    f"{conf.CX_CRN_ESQUEMA}.{conf.TL_CRN_BALANCE_MOVS}",
    "default",
    "append",
)
logger.info("Inserción completada correctamente")

# COMMAND ----------

# DBTITLE 3,LIMPIEZA Y FINALIZACIÓN
CleanUpManager.cleanup_notebook(locals())
logger.info("=== JOB COMPLETADO: NB_PANCIN_MOV_0035_BALANCE ===")
