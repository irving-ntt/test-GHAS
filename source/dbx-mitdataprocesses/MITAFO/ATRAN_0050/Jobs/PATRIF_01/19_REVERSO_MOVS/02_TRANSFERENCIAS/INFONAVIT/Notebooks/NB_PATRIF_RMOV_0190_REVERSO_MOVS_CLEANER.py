# Databricks notebook source
# MAGIC %md
# MAGIC # NB_PATRIF_0800_GMO_CLEANER
# MAGIC
# MAGIC **Propósito:** Limpieza de tablas Delta temporales generadas durante el proceso de generación de movimientos patrimoniales.
# MAGIC
# MAGIC **Descripción:** Este notebook elimina todas las tablas Delta temporales creadas por los notebooks de todos los procesos de generación de movimientos:
# MAGIC
# MAGIC ### 02_TRANSFERENCIAS/INFONAVIT
# MAGIC - NB_PATRIF_MOV_TRANS_INFO_01
# MAGIC - NB_PATRIF_MOV_TRANS_INFO_02
# MAGIC - NB_PATRIF_MOV_TRANS_TA_01
# MAGIC
# MAGIC ### 02_TRANSFERENCIAS/FOVISSSTE
# MAGIC - NB_PATRIF_0800_GMO_TRANS_FOV_01
# MAGIC
# MAGIC ### 03_DEV_SALDO_EXCED/INFONAVIT
# MAGIC - NB_PATRIF_GMO_0100_DEV_SALD_EXC
# MAGIC
# MAGIC ### 03_DEV_SALDO_EXCED/FOVISSSTE
# MAGIC - NB_PATRIF_GMO_0100_DEV_FOV
# MAGIC
# MAGIC ### 05_TRP/INFONAV_FOVISSS
# MAGIC - NB_PATRIF_MOV_TRANS_RP_01
# MAGIC
# MAGIC ### 06_DPSJL
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_01
# MAGIC - NB_PATRIF_MOV_DEV_PSJL_02 (7 notebooks modulares)
# MAGIC
# MAGIC **Total de tablas:** 29 tablas Delta temporales
# MAGIC
# MAGIC **Ejecución:** Debe ejecutarse al final del proceso para liberar espacio en el data lake.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cargar framework
# MAGIC %run "./startup"

# COMMAND ----------

# DBTITLE 1,Parametros originales
params = WidgetParams(
    {
        "sr_paso": str,
        "sr_etapa": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_subetapa": str,
        "sr_id_snapshot": str,
        "sr_folio": str,
        "sr_subproceso": str,
        "sr_fec_liq": str,
        "sr_fec_acc": str,
        "sr_proceso": str,
    }
)
params.validate()

# COMMAND ----------

# DBTITLE 1,Objeto para manejo del datalake
db = DBXConnectionManager()

# COMMAND ----------

# DBTITLE 1,Borrado de deltas


# COMMAND ----------

CleanUpManager.cleanup_notebook(locals())
