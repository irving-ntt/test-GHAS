# Databricks notebook source
'''
Descripcion:
    Depuración de tablas delta creadas y notificación para el flujo de Cifras Control Traspasos INFONAVIT.
Subetapa:
    26 - Cifras Control
Trámite:
    364 - Transferencias de Acreditados Infonavit
    365 - Transferencias por Anualidad Garantizada
    368 - Uso de Garantía por 43 BIS
Tablas input:
    N/A
Tablas output:
    N/A
Tablas delta:
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

DELTA_DS_700 = "DELTA_TI_DS_700_" + params.sr_id_archivo
DELTA_CONTEO = "DELTA_PROC_CONTEO_" + params.sr_id_archivo
DELTA_JOIN = "DELTA_TRANS_200_TI_JOIN_" + params.sr_id_archivo
DELTA_FL_400_SUBCTA = f"DELTA_FL_400_SUBCTA_{params.sr_id_archivo}"
DELTA_MOV_SUBCTA = f"DELTA_MOV_SUBCTA_{params.sr_id_archivo}"

db.drop_delta(DELTA_DS_700)
db.drop_delta(DELTA_CONTEO)
db.drop_delta(DELTA_JOIN)
db.drop_delta(DELTA_FL_400_SUBCTA)
db.drop_delta(DELTA_MOV_SUBCTA)
