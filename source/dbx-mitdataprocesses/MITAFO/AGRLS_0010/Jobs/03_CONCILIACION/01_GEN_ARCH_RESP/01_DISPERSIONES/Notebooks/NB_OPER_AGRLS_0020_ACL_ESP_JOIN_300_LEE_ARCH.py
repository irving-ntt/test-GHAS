# Databricks notebook source
'''
Descripcion:
    Proceso para la generación de la linea 2 del archivo de respuesta
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
    DELTA_ACLARACIONES_ESPECIALES_LEE_ARCH_#SR_ID_ARCHIVO#
    DELTA_ACLARACIONES_ESPECIALES_300_#SR_ID_ARCHIVO#
Tablas OUTPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_JOIN_300_LEE_ARCH_#SR_ID_ARCHIVO#
Archivos SQL:
    ACLARACIONES_ESPECIALES_007.sql
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

DELTA_TABLE_300 = "DELTA_ACLARACIONES_ESPECIALES_300_" + params.sr_id_archivo

DELTA_TABLE_LEE_ARCH = "DELTA_ACLARACIONES_ESPECIALES_LEE_ARCH_" + params.sr_id_archivo

# COMMAND ----------

# DBTITLE 1,VALIDACION 01
#Ejecuta consulta para el 01
statement_001 = query.get_statement(
    "ACLARACIONES_ESPECIALES_007.sql",
    DELTA_ACL_ESP_300=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_300}",
    DELTA_ACL_ESP_LEE_ARCH=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_LEE_ARCH}",
    SR_SUBPROCESO = params.sr_subproceso
)

#Ejecuta la consulta sobre la delta
df = db.sql_delta(query=statement_001)

DELTA_TABLE_001 = "DELTA_ACLARACIONES_ESPECIALES_JOIN_300_LEE_ARCH_" + params.sr_id_archivo

#Escribe delta
db.write_delta(DELTA_TABLE_001, df, "overwrite")

# COMMAND ----------

# DBTITLE 1,Tabla Resultados JOIN 300
if conf.debug:
    display(df)
