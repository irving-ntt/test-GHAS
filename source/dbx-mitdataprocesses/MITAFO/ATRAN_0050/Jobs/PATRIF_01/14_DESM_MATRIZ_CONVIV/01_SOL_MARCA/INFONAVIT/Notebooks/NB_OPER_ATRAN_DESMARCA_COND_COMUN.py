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
    "sr_tipo_ejecucion": str,
    #valores obligatorios
    "sr_etapa": str,
    "sr_instancia_proceso": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()



# COMMAND ----------

stage = 0

if params.sr_tipo_ejecucion == "0" or params.sr_tipo_ejecucion == "1":
    stage="DESMARCA COMUN"
elif params.sr_tipo_ejecucion == "2" :
    stage="INTEGRITY"



dbutils.jobs.taskValues.set(key = "sr_stage", value=stage)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_conv_ingty", value = params.sr_conv_ingty)
dbutils.jobs.taskValues.set(key = "sr_tipo_ejecucion", value = params.sr_conv_ingty)
dbutils.jobs.taskValues.set(key = "sr_path_arch", value = params.sr_path_arch)
dbutils.jobs.taskValues.set(key = "sr_usuario", value=params.sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_etapa", value=params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value=params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value=params.sr_id_snapshot)


# COMMAND ----------


