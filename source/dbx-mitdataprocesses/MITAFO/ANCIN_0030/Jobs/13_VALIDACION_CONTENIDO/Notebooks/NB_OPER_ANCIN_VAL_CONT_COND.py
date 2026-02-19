# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_path_arch": str,
    "sr_subproceso": str,
    "sr_origen_arc": str,
    "sr_id_archivo": str,
    "sr_subetapa": str,
    "sr_folio" :str,
    "sr_instancia_proceso" :str,
    "sr_usuario" :str,
    "sr_etapa" :str,
    "sr_id_snapshot" :str,
    "sr_paso" :str,
    "sr_id_archivo_siguiente" :str,
    "sr_fec_arc": str
})

# Validar widgets
#params.validate()

# COMMAND ----------

stage = 0
if params.sr_subetapa == "23":
    if params.sr_subproceso == '354' :
        stage = "INFONAVIT_SOL_MARCA"
    elif params.sr_subproceso == '347' : 
        stage = "INFONAVIT_SOL_DESMARCA"
    elif params.sr_subproceso == '364' :
        stage = "INFONAVIT_TRANS"     
    elif params.sr_subproceso == '365' :
        stage = "INFONAVIT_TRANS_ANUAL"   
    elif params.sr_subproceso == '368' :
        stage = "INFONAVIT_USO_GAR" 
    elif params.sr_subproceso == '348' :
        stage = "INFONAVIT_DEV_SAL_EX"  
    elif params.sr_subproceso == '349' :
        stage = "INFONAVIT_DEV_SAL_EX_AG_TA"  
    elif params.sr_subproceso == '3832' :
        stage = "INFONAVIT_DEV_PAGO_SJL"
    elif params.sr_subproceso == '363' :
        stage = "FOVISSTE_TRANS_ACRE"   
    elif params.sr_subproceso == '3286' :
        stage = "FOVISSTE_RECURSOS"   
    elif params.sr_subproceso == '350' :
        stage = "FOVISSTE_DEV_EXCE"           
    elif params.sr_subproceso == '3283' :
        stage = "FOVISSTE_DESMARCA"
    elif params.sr_subproceso == '69' :
        stage = "ARCHIVO_69"
elif params.sr_subetapa in ["25", "4030"]:
    if params.sr_subproceso == '354' :
        stage = "INFONAVIT_SOL_MARCA"
    elif params.sr_subproceso == '364' :
        stage = "INFONAVIT_TRANS"
    elif params.sr_subproceso == '365' :
        stage = "INFONAVIT_TRANS_ANUAL"
    elif params.sr_subproceso == '368' :
        stage = "INFONAVIT_USO_GAR"
    elif params.sr_subproceso == '347' :
        stage = "INFONAVIT_SOL_DESMARCA"
    elif params.sr_subproceso == '363' :
        stage = "FOVISSTE_TRANS_ACRE"
    elif params.sr_subproceso == '3286' :
        stage = "FOVISSTE_RECURSOS"
    elif params.sr_subproceso == '348' :
        stage = "INFONAVIT_DEV_SAL_EX"
    elif params.sr_subproceso == '350' :
        stage = "FOVISSTE_DEV_EXCE"
    elif params.sr_subproceso == '349' :
        stage = "INFONAVIT_DEV_SAL_EX_AG_TA"
    elif params.sr_subproceso == '3283' :
        stage = "FOVISSTE_DESMARCA"
    elif params.sr_subproceso == '3832' :
        stage = "INFONAVIT_DEV_PAGO_SJL"
#ARCHIVO DE RESPUESTA
elif params.sr_subetapa == "832":
    if params.sr_subproceso == '118' :
        stage = "ARCHIVO DE RESPUESTA ACLESP"
    elif params.sr_subproceso == '122' :
        stage = "ARCHIVO DE RESPUESTA ITISSSTE"
    elif params.sr_subproceso == '120' :
        stage = "ARCHIVO DE RESPUESTA DOISSSTE"
#RECHAZOS
elif params.sr_subetapa == "3356":
    if params.sr_subproceso == '354' :
        stage = "INFONAVIT_SOL_MARCA"
    elif params.sr_subproceso == '347' : 
        stage = "INFONAVIT_SOL_DESMARCA"
    elif params.sr_subproceso == '364' :
        stage = "INFONAVIT_TRANS"     
    elif params.sr_subproceso == '365' :
        stage = "INFONAVIT_TRANS_ANUAL"   
    elif params.sr_subproceso == '368' :
        stage = "INFONAVIT_USO_GAR" 
    elif params.sr_subproceso == '348' :
        stage = "INFONAVIT_DEV_SAL_EX"  
    elif params.sr_subproceso == '349' :
        stage = "INFONAVIT_DEV_SAL_EX_AG_TA"  
    elif params.sr_subproceso == '3832' :
        stage = "INFONAVIT_DEV_PAGO_SJL"
    elif params.sr_subproceso == '363' :
        stage = "FOVISSTE_TRANS_ACRE"
    elif params.sr_subproceso == '3286' :
        stage = "FOVISSTE_RECURSOS"   
    elif params.sr_subproceso == '350' :
        stage = "FOVISSTE_DEV_EXCE"           
    elif params.sr_subproceso == '3283' :
        stage = "FOVISSTE_DESMARCA"


dbutils.jobs.taskValues.set(key = "sr_stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_path_arch", value = params.sr_path_arch)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_origen_arc", value = params.sr_origen_arc)
dbutils.jobs.taskValues.set(key = "sr_id_archivo", value = params.sr_id_archivo)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
if params.sr_folio != None:
    dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
    dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
    dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
    dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
    dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
    dbutils.jobs.taskValues.set(key = "sr_paso", value = params.sr_paso)
    dbutils.jobs.taskValues.set(key = "sr_id_archivo_siguiente", value = params.sr_id_archivo_siguiente)
    dbutils.jobs.taskValues.set(key = "sr_fec_arc", value = params.sr_fec_arc)
