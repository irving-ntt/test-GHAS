# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams(
    {
        "sr_proceso": str,
        "sr_subproceso": str,
        "sr_subetapa": str,
        "sr_origen_arc": str,
        "sr_dt_org_arc": str,
        "sr_folio": str,
        "sr_id_archivo": str,
        "sr_instancia_proceso": str,
        "sr_usuario": str,
        "sr_etapa": str,
        "sr_id_snapshot": str,
        "sr_paso": str,
    }
)
params.validate()

# COMMAND ----------

Notify.send_notification("INFO", params)