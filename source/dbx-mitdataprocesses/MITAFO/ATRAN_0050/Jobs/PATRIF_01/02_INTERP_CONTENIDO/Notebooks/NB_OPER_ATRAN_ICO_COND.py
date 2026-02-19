# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_tipo_archivo": str,
    "sr_id_archivo": str,
    "sr_mask_rec_trp": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

stage = 0

if params.sr_subproceso == '354' :
    stage = "SDM"
elif params.sr_subproceso == '364' :
    stage = "STA"
elif params.sr_subproceso == '365' :
    stage = "SAG"
elif params.sr_subproceso == '368' :
    stage = "SUG"
elif params.sr_subproceso == '347' :
    stage = "SDD"
elif params.sr_subproceso == '363' :
    stage = "FOV"
elif params.sr_subproceso == '3286' :
    stage = "TRP"
elif params.sr_subproceso == '348' :
    stage = "DSE_UG"
elif params.sr_subproceso == '350' :
    stage = "DSEF"
elif params.sr_subproceso == '349' :
    stage = "DSE_AG_TA"
elif params.sr_subproceso == '3283' :
    stage = "SDDF"
elif params.sr_subproceso == '3832' :
    stage = "DPSJL"

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
dbutils.jobs.taskValues.set(key = "sr_tipo_archivo", value = params.sr_tipo_archivo)
dbutils.jobs.taskValues.set(key = "sr_mask_rec_trp", value = params.sr_mask_rec_trp)
