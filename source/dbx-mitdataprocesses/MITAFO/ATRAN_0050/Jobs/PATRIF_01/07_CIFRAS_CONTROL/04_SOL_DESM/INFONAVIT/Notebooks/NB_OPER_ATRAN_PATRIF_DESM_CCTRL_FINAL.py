# Databricks notebook source
"""
Descripcion:
    Extrae la información de Solicitud de Desmarca para obtención de Cifras Control e inserción a Tabla Cifras Control
Subetapa: 
    26 - Cifras Control
Trámite:
    347 - Desmarca de crédito de vivienda por 43 BIS
Tablas input:
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
Tablas output:
    TLAFOGRAL_VAL_CIFRAS_CTRL_RESP
Tablas Delta:
    N/A
Archivos SQL:
    100_DESM_CCTRL.sql
"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

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

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

Notify.send_notification("INFO", params)
