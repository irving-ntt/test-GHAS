# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

conf = ConfManager()


# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_path_arch": str,
    "sr_origen_arc": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})


# COMMAND ----------

Notify.send_notification("INFO", params)
