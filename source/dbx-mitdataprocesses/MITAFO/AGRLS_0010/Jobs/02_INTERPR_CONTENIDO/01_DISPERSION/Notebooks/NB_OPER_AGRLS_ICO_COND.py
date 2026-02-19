# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

stage = 0

if params.sr_proceso == '7' and params.sr_subproceso == '8' :
    stage = "DOIMSS"
elif params.sr_proceso == '7' and params.sr_subproceso == '120' :
    stage = "DOISSSTE"
elif params.sr_proceso == '7' and params.sr_subproceso == '122' :
    stage = "INT_ISSSTE"
elif params.sr_proceso == '7' and params.sr_subproceso == '121' :
    stage = "DITIMSS"
elif params.sr_proceso == '7' and params.sr_subproceso == '118' :
    stage = "AEIM"
elif params.sr_proceso == '7' and (params.sr_subproceso == '439'or params.sr_subproceso == '210') :
    stage = "DGG"

print(stage)

dbutils.jobs.taskValues.set(key = "sr_stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
dbutils.jobs.taskValues.set(key = "sr_id_archivo", value = params.sr_id_archivo)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
