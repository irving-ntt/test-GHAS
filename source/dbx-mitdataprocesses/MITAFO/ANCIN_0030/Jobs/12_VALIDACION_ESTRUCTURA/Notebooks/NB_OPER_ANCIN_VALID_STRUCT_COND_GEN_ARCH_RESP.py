# Databricks notebook source
'''
Descripcion:
    Validacion de estructura condicional
Subetapa:
    Validacion de estructura, Generacion de Archivo de Respuesta
Tramite:
    Todos los tramites
Tablas Input:
    NA
Tablas Output:
    NA
Tablas DELTA:
    NA

Archivos SQL:
    NA
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# Crear la instancia con los parámetros esperados
params = WidgetParams({    
    "sr_proceso": str,    
    "sr_subproceso": str,      
    "sr_subetapa": str,
    "sr_id_archivo":str,
    "sr_path_arch" :str,
    "sr_folio" :str,
    "sr_instancia_proceso" :str,
    "sr_usuario" :str,
    "sr_etapa" :str,
    "sr_id_snapshot" :str,
    "sr_paso" :str,
    "sr_id_archivo_siguiente" :str,
    "sr_fec_arc": str
})



# COMMAND ----------

dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_id_archivo", value = params.sr_id_archivo)
dbutils.jobs.taskValues.set(key = "sr_path_arch", value = params.sr_path_arch)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
if params.sr_folio != None:
    dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
    dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
    dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
    dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
    dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
    dbutils.jobs.taskValues.set(key = "sr_paso", value = params.sr_paso)
    dbutils.jobs.taskValues.set(key = "sr_id_archivo_siguiente", value = params.sr_id_archivo_siguiente)
