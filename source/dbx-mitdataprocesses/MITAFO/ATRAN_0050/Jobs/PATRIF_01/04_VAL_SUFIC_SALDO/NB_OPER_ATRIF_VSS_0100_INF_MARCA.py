# Databricks notebook source
"""
Descripción:
El trabajo realiza la extracción de información relevante para las solicitudes de marca en Infonavit desde una base de datos Oracle (OCI). Los datos extraídos se transforman según las necesidades del negocio y luego se almacenan en un DataSet para ser utilizados posteriormente en análisis o procesos adicionales.

Subetapa:
100 - Extracción y Transformación de Información de Infonavit

Trámite:
0100 - Solicitud de Marca para Cuentas INFONAVIT
Tablas Input:
CIERREN_ETL.TTAFOTRAS_ETL_MARCA_DESMARCA
Contiene información base que relaciona las marcas y desmarcas del proceso ETL.
CIERREN_ETL.TCAFOGRAL_VALOR_ACCION
Incluye detalles relacionados con fechas y valores de acciones relevantes para los registros.  
PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
Proporciona datos para la correlación entre marcas y desmarcas aplicadas a las cuentas.


"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_usuario": str,
    "sr_tipo_mov": str,
    "sr_conv_ingty": str,
    "sr_path_arch": str,
    "sr_tipo_ejecucion": str,
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

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_100_MARCA_DESMARCA_INFO.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_100_MARCA_DESMARCA_INFO_{params.sr_folio}"))

# COMMAND ----------


