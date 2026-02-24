# Databricks notebook source
'''
Descripcion:
    Notebook condicionale de SOL MARCA evaluando el parametro sr_recalculo
Subetapa:
    26 - CIFRAS CONTROL
Trámite:
    354 - IMSS Solicitud de Marca de Cuentas por 43 BIS
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    N/A
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

# 📌 Definir y validar parámetros de entrada
# Se definen los parámetros requeridos para el proceso
# Crear la instancia con los parámetros esperados
params = WidgetParams({
    "sr_etapa":str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_id_snapshot":str,
    "sr_proceso":str,
    "sr_subproceso": str,
    "sr_recalculo":str,
    "tipo_arch":str,
    "sr_folio": str,
    })
# Validar widgets
params.validate()

# COMMAND ----------

if params.sr_recalculo == 0 :
    stage = "SOL MARCA RECALCULO 0"
elif params.sr_subproceso == 1:
    stage = "SOL MARCA RECALCULO 1"

dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_recalculo", value = params.sr_recalculo)
dbutils.jobs.taskValues.set(key = "tipo_arch", value = params.tipo_arch)
