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

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

# COMMAND ----------

Notify.send_notification("INFO", params)

# COMMAND ----------

DELTA_TABLE_CARGA_INI = "DELTA_ACLARACIONES_ESPECIALES_CARGA_INI_" + params.sr_id_archivo
DELTA_TABLE_LEE_ARCH = "DELTA_ACLARACIONES_ESPECIALES_LEE_ARCH_" + params.sr_id_archivo
DELTA_TABLE_JOIN_300 = "DELTA_ACLARACIONES_ESPECIALES_JOIN_300_LEE_ARCH_" + params.sr_id_archivo
DELTA_TABLE_ARCH_RESP = "DELTA_ACLARACIONES_ESPECIALES_ARCH_RESP_" + params.sr_id_archivo
DELTA_TABLE_200 = "DELTA_ACLARACIONES_ESPECIALES_200_" + params.sr_id_archivo
DELTA_TABLE_300 = "DELTA_ACLARACIONES_ESPECIALES_300_"+ params.sr_id_archivo
DELTA_TABLE_400 = "DELTA_ACLARACIONES_ESPECIALES_400_" + params.sr_id_archivo
DELTA_TABLE_500 = "DELTA_ACLARACIONES_ESPECIALES_500_" + params.sr_id_archivo

DELTA_TABLE_47_INCO = "DELTA_INCO_INT_TOT_" + params.sr_folio

db.drop_delta(DELTA_TABLE_CARGA_INI)
db.drop_delta(DELTA_TABLE_LEE_ARCH)
db.drop_delta(DELTA_TABLE_JOIN_300)
db.drop_delta(DELTA_TABLE_ARCH_RESP)
db.drop_delta(DELTA_TABLE_200)
db.drop_delta(DELTA_TABLE_300)
db.drop_delta(DELTA_TABLE_400)
db.drop_delta(DELTA_TABLE_500)

db.drop_delta(DELTA_TABLE_47_INCO)

