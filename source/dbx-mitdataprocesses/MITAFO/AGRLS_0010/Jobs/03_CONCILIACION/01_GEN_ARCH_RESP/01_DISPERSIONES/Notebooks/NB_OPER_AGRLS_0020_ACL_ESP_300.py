# Databricks notebook source
'''
Descripcion:
    Generación de la Tabla DELTA300 para el trámite 118
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    118 - DAEIMSS
Tablas INPUT:
    N/A
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_CARGA_INI_#SR_ID_ARCHIVO#
Tablas OUTPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_300_#SR_ID_ARCHIVO#
Archivos SQL:
    ACLARACIONES_ESPECIALES_004.sql
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
    "sr_id_snapshot": str
})
# Validar widgets
params.validate()

# COMMAND ----------

# 📌 Cargar configuraciones globales
# Se establecen variables de configuración necesarias para el proceso
conf = ConfManager()

db = DBXConnectionManager()

query = QueryManager()

file_manager = FileManager(err_repo_path=conf.err_repo_path)

# COMMAND ----------

DELTA_TABLE_001 = "DELTA_ACLARACIONES_ESPECIALES_CARGA_INI_" + params.sr_id_archivo
DELTA_TABLE_300 = "DELTA_ACLARACIONES_ESPECIALES_300_" + params.sr_id_archivo

# COMMAND ----------

# DBTITLE 1,VALIDACION 01
#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "ACLARACIONES_ESPECIALES_004.sql",
    DELTA_TABLE_001=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_001}"
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

db.write_delta(DELTA_TABLE_300, df, "overwrite")

# COMMAND ----------

if conf.debug:
    display(df)
