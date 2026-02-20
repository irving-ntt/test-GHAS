# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_MOV_DEV_PSJL_02_05_EXTRACT_VAL_SALDOS
# MAGIC
# MAGIC **Descripción:** Extrae validación de saldos DPSJL desde Oracle con LEFT JOIN.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Pagos SJL
# MAGIC
# MAGIC **Stage Original:** DB_100_TL_PRO_DEV_PAG_SJL
# MAGIC
# MAGIC **Tablas Input (Oracle):**
# MAGIC - PROCESOS.TTCRXGRAL_DEV_PAG_SJL (main)
# MAGIC - PROCESOS.TTCRXGRAL_VAL_SDOS_DPSJL (left join)
# MAGIC
# MAGIC **Tabla Output (Delta):**
# MAGIC - TEMP_VAL_SALDOS_{sr_folio}
# MAGIC
# MAGIC **Archivo SQL:**
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_001_OCI_VAL_SALDOS.sql

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Definir parámetros dinámicos del notebook
params = WidgetParams(
    {
        "sr_actualiza": str,
        "sr_etapa": str,
        "sr_fec_acc": str,
        "sr_fec_liq": str,
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_id_snapshot": str,
        "sr_instancia_proceso": str,
        "sr_paso": str,
        "sr_proceso": str,
        "sr_reproceso": str,
        "sr_subetapa": str,
        "sr_subproceso": str,
        "sr_usuario": str,
    }
)
params.validate()

# Cargar configuración de entorno
conf = ConfManager()

# Inicializar managers
query = QueryManager()
db = DBXConnectionManager()

# COMMAND ----------

# MAGIC %md
# MAGIC <li>
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332253?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_001.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,Extraer Validación de Saldos DPSJL desde Oracle (DB_100_TL_PRO_DEV_PAG_SJL)
statement = query.get_statement(
    "NB_PATRIF_MOV_DEV_PSJL_02_001_OCI_VAL_SALDOS.sql",
    CX_PRO_ESQUEMA=conf.CX_PRO_ESQUEMA,
    TL_PRO_DEV_PAG_SJL=conf.TL_PRO_DEV_PAG_SJL,
    TL_PRO_VAL_SDOS_DPSJL=conf.TL_PRO_VAL_SDOS_DPSJL,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

# Escribir datos de Oracle a tabla Delta temporal
db.write_delta(
    f"TEMP_VAL_SALDOS_{params.sr_folio}",
    db.read_data("default", statement),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_VAL_SALDOS_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
