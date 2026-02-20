# Databricks notebook source
'''
Descripcion:
    Carga inicial de datos para la generación de archivos.
Subetapa:
    20 - ARCHIVO RESPUESTA
Trámite:
    120 - DOISSSTE
    122 - DITISSSTE
Tablas INPUT:
    CIERREN_ETL.TTSISGRAL_ETL_DISPERSION
Tablas OUTPUT:
    N/A
Tablas INPUT DELTA:
    DELTA_INCO_INT_TOT_#FOLIO#
    DELTA_ACLARACIONES_ESPECIALES_100_#SR_FOLIO#
Tablas OUTPUT DELTA:
    DELTA_ACLARACIONES_ESPECIALES_100_#SR_ID_ARCHIVO#
Archivos SQL:
    ACLARACIONES_ESPECIALES_ISSSTE_001.sql
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

# COMMAND ----------

DELTA_TABLE_001 = "DELTA_ACLARACIONES_ESPECIALES_100_" + params.sr_id_archivo

# COMMAND ----------

#Leer desde OCI
statement_001 = query.get_statement(
    "ACLARACIONES_ESPECIALES_ISSSTE_001.sql",
    SR_FOLIO=params.sr_folio,
    P_PROCESO=params.sr_proceso,
    P_SUBPROCESO=params.sr_subproceso,
)
df = db.read_data("default", statement_001)

#Escribe delta
db.write_delta(DELTA_TABLE_001, df, "overwrite")

# COMMAND ----------

if conf.debug:
    display(df)
