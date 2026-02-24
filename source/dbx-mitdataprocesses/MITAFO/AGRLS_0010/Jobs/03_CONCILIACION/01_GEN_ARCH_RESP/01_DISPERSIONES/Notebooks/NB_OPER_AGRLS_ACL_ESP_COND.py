# Databricks notebook source
'''
Descripcion:
    Analiza el parámetro subproceso y, según su evaluación, determina automáticamente el workflow al que debe dirigirse.
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    118 - DAEIMSS
    120 - DOISSSTE
    122 - DITISSSTE
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
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio" : str,
    "sr_id_archivo": str,
    "sr_fec_arc" : str,
    "sr_fec_liq" : str,
    "sr_dt_org_arc" : str,
    "sr_origen_arc" : str,
    "sr_tipo_layout": str,
    "sr_tipo_reporte" : str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
    "sr_paso":str,
    })
# Validar widgets
params.validate()

# COMMAND ----------

if params.sr_subproceso == '120' :
    stage = "DOISSSTE"
elif params.sr_subproceso == '122' :
    stage = "INT_ISSSTE"
elif params.sr_subproceso == '118' :
    stage = "AEIM"

dbutils.jobs.taskValues.set(key = "sr_proceso", value = params.sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = params.sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = params.sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_folio", value = params.sr_folio)
dbutils.jobs.taskValues.set(key = "sr_id_archivo", value = params.sr_id_archivo)
dbutils.jobs.taskValues.set(key = "sr_fec_arc", value = params.sr_fec_arc)
dbutils.jobs.taskValues.set(key = "sr_fec_liq", value = params.sr_fec_liq)
dbutils.jobs.taskValues.set(key = "sr_dt_org_arc", value = params.sr_dt_org_arc)
dbutils.jobs.taskValues.set(key = "sr_origen_arc", value = params.sr_origen_arc)
dbutils.jobs.taskValues.set(key = "sr_tipo_layout", value = params.sr_tipo_layout)
dbutils.jobs.taskValues.set(key = "sr_tipo_reporte", value = params.sr_tipo_reporte)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = params.sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_usuario", value = params.sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_etapa", value = params.sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = params.sr_id_snapshot)
dbutils.jobs.taskValues.set(key = "sr_paso", value = params.sr_paso)
