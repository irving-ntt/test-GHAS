# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_MOV_DEV_PSJL_02_02_EXTRACT_TIPO_SUBCTA
# MAGIC
# MAGIC **Descripción:** Extrae catálogo de tipos de subcuenta desde Oracle.
# MAGIC
# MAGIC **Subetapa:** Generación de Movimientos - Devolución de Pagos SJL
# MAGIC
# MAGIC **Stage Original:** DB_400_TIPO_SUBCTA
# MAGIC
# MAGIC **Tabla Input (Oracle):**
# MAGIC - CIERREN.TCCRXGRAL_TIPO_SUBCTA
# MAGIC
# MAGIC **Tabla Output (Delta):**
# MAGIC - TEMP_TIPO_SUBCTA
# MAGIC
# MAGIC **Archivo SQL:**
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02_003_OCI_TIPO_SUBCTA.sql

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
# MAGIC   <a href="https://adb-3494755435873649.9.azuredatabricks.net/editor/files/502961855332256?o=3494755435873649" target="_blank">
# MAGIC     <img src="/Workspace/Repos/APPS/SRC/source/dbx-mitdataprocesses/MITAFO/ATRAN_0050/Jobs/PATRIF_01/08_GEN_MOVIMIENTOS/06_DPSJL/Notebooks/assets/NB_PATRIF_MOV_DEV_PSJL_02_003.png" style="max-width: 100%; height: auto;"/>
# MAGIC   </a>
# MAGIC </li>

# COMMAND ----------

# DBTITLE 1,Extraer Catálogo de Tipos de Subcuenta (DB_400_TIPO_SUBCTA)
statement = query.get_statement(
    "NB_PATRIF_MOV_DEV_PSJL_02_003_OCI_TIPO_SUBCTA.sql",
    CX_CRN_ESQUEMA=conf.CX_CRN_ESQUEMA,
    TL_CRN_TIPO_SUBCTA=conf.TL_CRN_TIPO_SUBCTA,
)

# Escribir catálogo a tabla Delta temporal
db.write_delta(
    "TEMP_TIPO_SUBCTA",
    db.read_data("default", statement),
    "overwrite",
)

if conf.debug:
    display(db.read_delta("TEMP_TIPO_SUBCTA"))

# COMMAND ----------

# DBTITLE 1,LIMPIEZA DE RECURSOS
CleanUpManager.cleanup_notebook(locals())
