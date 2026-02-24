# Databricks notebook source
'''
Descripcion:
    
Subetapa:
    Comunicaciones
Trámite:
    348	Devolución de Excedentes por 43 BIS
    349	Devolución de Saldos Excedentes por T.A.A.G.
    350	Devolución de Excedentes por T.A. Fovissste
    363	Transferencia de Acreditados Fovissste
    364	Transferencias de Acreditados Infonavit
    365	Transferencia por Anualidad Garantizada
    368	Uso de Garantía por 43 BIS
    3286 Transferencia de Recursos por Portabilidad
    3832 Devolución de Pago sin Justificación Legal

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

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    #"sr_mask_rec_trp": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

# Definir zona horaria de México
zona_mexico = ZoneInfo("America/Mexico_City")

# Obtener fecha y hora local
fecha = datetime.now(zona_mexico).strftime("%Y%m%d")

stage = 0

if params.sr_subproceso in ['348','349'] :
    stage = "DEVO"
elif params.sr_subproceso in ['364','365','368'] :
    stage = "TRANS"
elif params.sr_subproceso == '350':
    stage = "DSEF"
elif params.sr_subproceso == '3286':
    stage = "TRP"
elif params.sr_subproceso == '363':
    stage = "TRFO"
elif params.sr_subproceso == '3832':
    stage = "DPSJL"

print(stage,fecha)

#No hay necesidad de mandar nuevamente los parametros porque los parametros del WF sobreescriben los parametros de los nodos, entonces ya se tienen, es redundante.
dbutils.jobs.taskValues.set(key = "sr_stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_fecha", value = fecha)
#dbutils.jobs.taskValues.set(key = "sr_proceso" , value = params.sr_proceso)
#dbutils.jobs.taskValues.set(key = "sr_subproceso" , value = params.sr_subproceso)
#dbutils.jobs.taskValues.set(key = "sr_subetapa" , value = params.sr_subetapa)
#dbutils.jobs.taskValues.set(key = "sr_folio" , value = params.sr_folio)
#dbutils.jobs.taskValues.set(key = "sr_mask_rec_trp" , value = params.sr_mask_rec_trp)
#dbutils.jobs.taskValues.set(key = "sr_instancia_proceso" , value = params.sr_instancia_proceso)
#dbutils.jobs.taskValues.set(key = "sr_usuario" , value = params.sr_usuario)
#dbutils.jobs.taskValues.set(key = "sr_etapa" , value = params.sr_etapa)
#dbutils.jobs.taskValues.set(key = "sr_id_snapshot" , value = params.sr_id_snapshot)
