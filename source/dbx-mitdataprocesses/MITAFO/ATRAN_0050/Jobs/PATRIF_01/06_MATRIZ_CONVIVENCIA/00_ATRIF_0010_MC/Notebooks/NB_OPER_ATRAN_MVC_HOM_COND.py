# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_reproceso": int,
    "sr_conv_ingty":str,
    "sr_path_arch": str,
    "sr_etapa": str,
    "sr_tipo_mov":str,
    
})
# Validar widgets
params.validate()

# COMMAND ----------

stage = 0
sr_tipo_ejecucion=None

if  params.sr_reproceso == 1 or params.sr_reproceso==2 :
    sr_tipo_ejecucion="2"
elif params.sr_reproceso == 0:
    sr_tipo_ejecucion="1"
elif params.sr_reproceso == 3:
    sr_tipo_ejecucion="3"
else:
    "Parametros incorrectos"

print(sr_tipo_ejecucion)
dbutils.jobs.taskValues.set(key = "sr_tipo_ejecucion", value = sr_tipo_ejecucion)
dbutils.jobs.taskValues.set(key = "stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_reproceso", value = params.sr_reproceso)
dbutils.jobs.taskValues.set(key = "sr_conv_ingty", value = params.sr_conv_ingty)
dbutils.jobs.taskValues.set(key = "sr_path_arch", value = params.sr_path_arch)
dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_tipo_mov", value = params.sr_tipo_mov)

