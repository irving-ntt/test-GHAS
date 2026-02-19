# Databricks notebook source
'''
Descripcion:
    Proceso final de eliminación de tablas delta usadas para la creación de archivos.
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    118 - DAEIMSS
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
})
# Validar widgets
params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

# COMMAND ----------

Notify.send_notification("INFO", params)

# COMMAND ----------

DELTA_600_GEN_MOV = f"DELTA_600_GEN_MOV_{params.sr_folio}"
DELTA_900_CIF_RECH = DELTA_600_GEN_MOV=f"DELTA_600_GEN_MOV_{params.sr_folio}"
DELTA_TABLE_CONTEOS = f"DELTA_CONTEOS_{params.sr_folio}"
DELTA_TABLE_002 = f"DELTA_RECHAZOS_{params.sr_folio}"

db.drop_delta(DELTA_600_GEN_MOV)
db.drop_delta(DELTA_900_CIF_RECH)
db.drop_delta(DELTA_TABLE_CONTEOS)
db.drop_delta(DELTA_TABLE_002)
