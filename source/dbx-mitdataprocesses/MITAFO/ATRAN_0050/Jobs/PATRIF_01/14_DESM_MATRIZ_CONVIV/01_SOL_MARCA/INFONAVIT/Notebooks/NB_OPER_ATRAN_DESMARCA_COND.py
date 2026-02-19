# Databricks notebook source
'''
Descripcion:
    
Subetapa:
    
Tramite:
    
Tablas Input:
   
Tablas Output:
    
Tablas DELTA:
    

Archivos SQL:
 
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_path_arch": str,
    "sr_conv_ingty": str,
    #valores obligatorios
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()



# COMMAND ----------

stage = 0

if params.sr_subproceso == "354":
    stage="SOLICITUD DE MARCA DE CUENTAS POR 43 BIS"
elif params.sr_subproceso == "364" or params.sr_subproceso == "365" or params.sr_subproceso == "368" :
    stage="GENERAL PARA ACREDITADOS INFONAVIT ANUALIDAD Y 43 BIS"
elif params.sr_subproceso == "347" and params.sr_conv_ingty == "0":
    stage="DESMARCA DE CREDITO DE VIVIENDA POR 43 BIS"
elif params.sr_subproceso == "363":
    stage="TRANSFERENCIA DE ACREDITADOS FOVISSSTE"
elif params.sr_subproceso == "3286":
    stage="TRANSFERENCIA DE RECURSOS POR PORTABILIDAD"
elif params.sr_subproceso == "348" or  params.sr_subproceso == "349":
    stage="DEVOLUCION DE EXCEDENTES POR 43 BIS Y SALDOS EXCEDENTES POR T. A. A. G." 
elif params.sr_subproceso == "350":
    stage="DEVOLUCION DE EXCEDENTES POR T A FOVISSSTE"
elif params.sr_subproceso == "3283":
    stage="DESMARCA FOVISSSTE"
elif params.sr_subproceso == "3832":
    stage="DEVOLUCION DE PAGO SIN JUSTIFICACION LEGAL IMSS"
elif params.sr_subproceso == "347" and params.sr_conv_ingty == "1":
    stage="DESMARCA DE CREDITO DE VIVIENDA POR 43 BIS RECHAZADO"


dbutils.jobs.taskValues.set(key = "sr_stage", value=stage)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_conv_ingty", value = params.sr_conv_ingty)
dbutils.jobs.taskValues.set(key = "sr_path_arch", value = params.sr_path_arch)
dbutils.jobs.taskValues.set(key = "sr_usuario", value=params.sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_etapa", value=params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value=params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value=params.sr_id_snapshot)

