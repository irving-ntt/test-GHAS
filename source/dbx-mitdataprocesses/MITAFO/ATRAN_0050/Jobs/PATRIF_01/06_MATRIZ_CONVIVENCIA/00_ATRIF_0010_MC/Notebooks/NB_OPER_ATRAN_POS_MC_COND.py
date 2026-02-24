# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_tipo_ejecucion":int,
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot":str,
    "sr_paso":str,
})
# Validar widgets
params.validate()


# COMMAND ----------

stage = 0

if params.sr_subproceso == '354' :
    stage = "INFONAVIT_SOLICITUD_MC"
elif params.sr_subproceso == '364' :
    stage = "INFONAVIT_TRANSFERENCIA_ACRE"
elif params.sr_subproceso == '365' :
    stage = "INFONAVIT_TRANSFERENCIA_ANUA_GAR"
elif params.sr_subproceso == '368' :
    stage = "INFONAVIT_USO_GAR"
elif params.sr_subproceso == '347' :
    stage = "INFONAVIT_SOLICITUD_DESM"
elif params.sr_subproceso == '363' :
    stage = "FOVISSSTE_TRANSFERENCIA_ACRE"
elif params.sr_subproceso == '3286' :
    stage = "MIXTA_TRANSFERENCIA_RECURSOS_PORT"
elif params.sr_subproceso == '348' :
    stage = "INFONAVIT_DEVOLUCION_EXCE"
elif params.sr_subproceso == '350' :
    stage = "FOVISSSTE_DEVOLUCION_EXCE_TA"
elif params.sr_subproceso == '349' :
    stage = "INFONAVIT_DEVOLUCION_SALD_EXCE_TAAG"
elif params.sr_subproceso == '3283' :
    stage = "FOVISSSTE_DESMARCA"
elif params.sr_subproceso == '3832' :
    stage = "INFONAVIT_DEVOLUCION_PAG_SJL"

print(stage)
dbutils.jobs.taskValues.set(key = "sr_stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_tipo_ejecucion", value = params.sr_tipo_ejecucion)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
dbutils.jobs.taskValues.set(key = "sr_paso", value = params.sr_paso)


# COMMAND ----------


