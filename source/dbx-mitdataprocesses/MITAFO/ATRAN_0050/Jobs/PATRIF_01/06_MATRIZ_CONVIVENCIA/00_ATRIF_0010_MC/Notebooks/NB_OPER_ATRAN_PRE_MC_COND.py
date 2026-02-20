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
    "sr_tipo_mov": str,
    "sr_paso":str,
})
# Validar widgets
params.validate()

# COMMAND ----------

stage = 0

if params.sr_subproceso == '354' and params.sr_tipo_ejecucion==0 :
    stage = "INFONAVIT_SOLICITUD_MC"
elif params.sr_subproceso == '354' and params.sr_tipo_ejecucion==1:
    stage = "INFONAVIT_SOLICITUD_MC_01_REP"
elif params.sr_subproceso == '354' and params.sr_tipo_ejecucion==2:
    stage = "INFONAVIT_SOLICITUD_MC_02_REP"
elif (params.sr_subproceso == '364' or params.sr_subproceso == '365' or params.sr_subproceso == '368') and params.sr_tipo_ejecucion==0:
    stage = "INFONAVIT_TRANSFERENCIAS"
elif (params.sr_subproceso == '364' or params.sr_subproceso == '365' or params.sr_subproceso == '368') and params.sr_tipo_ejecucion==1:
    stage = "INFONAVIT_TRANSFERENCIAS_01_REP"
elif (params.sr_subproceso == '364' or params.sr_subproceso == '365' or params.sr_subproceso == '368') and params.sr_tipo_ejecucion==2:
    stage = "INFONAVIT_TRANSFERENCIAS_02_REP"
elif params.sr_subproceso == '347' and params.sr_tipo_ejecucion==0 :
    stage = "INFONAVIT_SOLICITUD_DESM"
elif params.sr_subproceso == '347' and params.sr_tipo_ejecucion==1:
    stage = "INFONAVIT_SOLICITUD_DESM_01_REP"
elif params.sr_subproceso == '347' and params.sr_tipo_ejecucion==2:
    stage = "INFONAVIT_SOLICITUD_DESM_02_REP"
elif (params.sr_subproceso == '349' or params.sr_subproceso == '348') and params.sr_tipo_ejecucion==0:
    stage = "INFONAVIT_DEVOLUCION_EXCEDENTES"
elif (params.sr_subproceso == '349' or params.sr_subproceso == '348') and params.sr_tipo_ejecucion==1:
    stage = "INFONAVIT_DEVOLUCION_EXCEDENTES_01_REP"
elif (params.sr_subproceso == '349' or params.sr_subproceso == '348') and params.sr_tipo_ejecucion==2:
    stage = "INFONAVIT_DEVOLUCION_EXCEDENTES_02_REP"
elif params.sr_subproceso == '3832' and params.sr_tipo_ejecucion==0:
    stage = "INFONAVIT_DEVOLUCION_PAG_SJL"
elif params.sr_subproceso == '3832' and params.sr_tipo_ejecucion==1:
    stage = "INFONAVIT_DEVOLUCION_PAG_SJL_01_REP"
elif params.sr_subproceso == '3832' and params.sr_tipo_ejecucion==2:
    stage = "INFONAVIT_DEVOLUCION_PAG_SJL_02_REP"
elif params.sr_subproceso == '3286'  and params.sr_tipo_ejecucion==0 :
    stage = "MIXTA_TRANSFERENCIA_RECURSOS_PORT"
elif params.sr_subproceso == '3286'  and params.sr_tipo_ejecucion==1 :
    stage = "MIXTA_TRANSFERENCIA_RECURSOS_PORT_01_REP"
elif params.sr_subproceso == '3286'  and params.sr_tipo_ejecucion==2 :
    stage = "MIXTA_TRANSFERENCIA_RECURSOS_PORT_02_REP"
elif params.sr_subproceso == '363'and params.sr_tipo_ejecucion==0:
    stage = "FOVISSSTE_TRANSFERENCIA_ACRE"
elif params.sr_subproceso == '363'and params.sr_tipo_ejecucion==1:
    stage = "FOVISSSTE_TRANSFERENCIA_ACRE_01_REP"
elif params.sr_subproceso == '363'and params.sr_tipo_ejecucion==2:
    stage = "FOVISSSTE_TRANSFERENCIA_ACRE_02_REP"
elif params.sr_subproceso == '350' and params.sr_tipo_ejecucion==0:
    stage = "FOVISSSTE_DEVOLUCION_EXCE_TA"
elif params.sr_subproceso == '350' and params.sr_tipo_ejecucion==1 :
    stage = "FOVISSSTE_DEVOLUCION_EXCE_TA_01_REP"
elif params.sr_subproceso == '351' and params.sr_tipo_ejecucion==2:
    stage = "FOVISSSTE_DEVOLUCION_EXCE_TA_02_REP"
elif params.sr_subproceso == '3283' and params.sr_tipo_ejecucion==0:
    stage = "FOVISSSTE_DESMARCA"
elif params.sr_subproceso == '3283' and params.sr_tipo_ejecucion==1:
    stage = "FOVISSSTE_DESMARCA_01_REP"
elif params.sr_subproceso == '3283' and params.sr_tipo_ejecucion==2:
    stage = "FOVISSSTE_DESMARCA_02_REP"

print(stage)
dbutils.jobs.taskValues.set(key = "sr_stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_tipo_ejecucion", value = params.sr_tipo_ejecucion)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
dbutils.jobs.taskValues.set(key = "sr_tipo_mov", value = params.sr_tipo_mov)
dbutils.jobs.taskValues.set(key = "sr_paso", value = params.sr_paso)


# COMMAND ----------


