# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_folio": str,
    "sr_proceso": str,
    "sr_reproceso": str,
    "sr_subetapa": str,
    "sr_subproceso": str,
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

stage = 0

if params.sr_subproceso == '368' :
    stage = "INFONAVIT_TRANS"
elif params.sr_subproceso == '364' : 
    stage = "INFONAVIT_TRANS"
elif params.sr_subproceso == '365' :
    stage = "INFONAVIT_TRANS"     
   

         

print(stage)
dbutils.jobs.taskValues.set(key = "stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_reproceso", value = params.sr_reproceso)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)

