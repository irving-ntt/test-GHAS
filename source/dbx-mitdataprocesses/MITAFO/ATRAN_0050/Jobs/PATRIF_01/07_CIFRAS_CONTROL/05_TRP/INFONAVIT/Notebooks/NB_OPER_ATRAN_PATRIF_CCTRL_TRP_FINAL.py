# Databricks notebook source
'''
Descripcion:
    Depuración de tablas delta creadas para el flujo de Cifras Control Transferencia de Recursos de Portabilidad.
Subetapa: 
    26 - Cifras Control
Trámite:
    3286 - Transferencia de Recursos por portabilidad
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas Delta:
    DELTA_300_JOIN_CTA_INDV_{params.sr_folio}
    DELTA_600_TRANS_{params.sr_folio}
    DELTA_PROC_CONTEO_{params.sr_folio}
    DELTA_MOV_SUBMARCA_{params.sr_folio}
    DELTA_JO_400_{params.sr_folio}
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
    "sr_proceso":str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_etapa":str,
    "sr_id_archivo": str,
    "sr_instancia_proceso":str,
    "sr_usuario":str,
    "sr_id_snapshot":str,
    "sr_recalculo":str,
    "sr_tipo_archivo":str,
    "sr_tipo_layout":str,
})
# Validar widgets
# params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

# COMMAND ----------

Notify.send_notification("INFO", params)

# COMMAND ----------

DELTA_300_JOIN_CTA_INDV = f"DELTA_300_JOIN_CTA_INDV_{params.sr_folio}"
DELTA_600_TRANS = f"DELTA_600_TRANS_{params.sr_folio}"
DELTA_PROC_CONTEO = f"DELTA_PROC_CONTEO_{params.sr_folio}"
DELTA_MOV_SUBMARCA = f"DELTA_MOV_SUBMARCA_{params.sr_folio}"
DELTA_JO_400 = f"DELTA_JO_400_{params.sr_folio}"

db.drop_delta(DELTA_300_JOIN_CTA_INDV)
db.drop_delta(DELTA_600_TRANS)  
db.drop_delta(DELTA_PROC_CONTEO)
db.drop_delta(DELTA_MOV_SUBMARCA)
db.drop_delta(DELTA_JO_400)
