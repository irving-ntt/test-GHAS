# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_MOV_DEV_PSJL_02_03_EXTRACT_MOV_SUBCTA
# MAGIC
# MAGIC **Descripción:** Extrae configuración de movimientos de subcuenta con deducibilidad desde Oracle.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Pagos SJL
# MAGIC
# MAGIC **Stage Original:** DB_500_MOV_SUBCTA
# MAGIC
# MAGIC **Tablas Input (Oracle):**
# MAGIC - CIERREN.TRAFOGRAL_MOV_SUBCTA
# MAGIC - CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV
# MAGIC
# MAGIC **Tabla Output (Delta):**
# MAGIC - TEMP_MOV_SUBCTA_{sr_subproceso}
# MAGIC
# MAGIC **Archivo SQL:**
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_004_OCI_MOV_SUBCTA.sql

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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332255?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_004.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,Extraer Config Movimientos Subcuenta con Deducibilidad (DB_500_MOV_SUBCTA)
statement = query.get_statement(
    "NB_PATRIF_MOV_DEV_PSJL_02_004_OCI_MOV_SUBCTA.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_MOV_SUBCTA=conf.TL_CRN_MOV_SUBCTA,
    TL_CRN_CONFIG_CONCEP_MOV=conf.TL_CRN_CONFIG_CONCEP_MOV,
    SR_SUBPROCESO=params.sr_subproceso,
)

# Escribir config a tabla Delta temporal (filtrada por subproceso)
db.write_delta(
    f"TEMP_MOV_SUBCTA_{params.sr_subproceso}",
    db.read_data("default", statement),
    "overwrite",
)

if conf.debug:
    display(db.read_delta(f"TEMP_MOV_SUBCTA_{params.sr_subproceso}"))

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
