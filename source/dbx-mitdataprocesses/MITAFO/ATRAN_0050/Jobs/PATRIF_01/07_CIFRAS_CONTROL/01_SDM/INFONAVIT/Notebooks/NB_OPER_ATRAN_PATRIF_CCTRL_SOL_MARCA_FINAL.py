# Databricks notebook source
'''
Descripcion:
    Proceso final de eliminación de tablas delta usadas para la generación de cifras.
Subetapa:
    26 - Generación de Cifras Control
Trámite:
    354 - IMSS Solicitud de marca de cuentas por 43 bis
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

#DELTA_TABLE_MARCA_DESMARCA_RECH = "DELTA_MARCA_DESMARCA_RECH_CONTEO_" + params.sr_id_archivo
DELTA_TABLE_MARCA_DESMARCA = "DELTA_100_MARCA_DESMARCA_" + params.sr_id_archivo

#db.drop_delta(DELTA_TABLE_MARCA_DESMARCA_RECH)
db.drop_delta(DELTA_TABLE_MARCA_DESMARCA)

