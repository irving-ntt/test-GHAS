# Databricks notebook source
"""
Descripcion:
    Se realiza la extracción de la información de Devolución de pagos SJL para obtener las cifras control.
Subetapa: 
    26 - Cifras Control
Trámite:
    3832 - Devolución de pago sin Justificación Legal
Tablas input:
    PROCESOS.TTCRXGRAL_DEV_PAG_SJL
Tablas output:
    N/A
Tablas Delta:
    DELTA_CONTEOS_{params.sr_folio}
    DELTA_TRANSFORMADA_{params.sr_folio}
Archivos SQL:
    400_TL_PRO_DEV_PAG_SJL.sql
    200_TOT_DPSJL_JOIN_CONTEOS.sql
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

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "400_TL_PRO_DEV_PAG_SJL.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

DELTA_TABLE_001 = f"DELTA_TRANSFORMADA_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001,db.read_data("default",statement) , "overwrite")

# COMMAND ----------

if conf.debug:
    display(db.read_delta(DELTA_TABLE_001))

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "200_TOT_DPSJL_JOIN_CONTEOS.sql",
    DELTA_RECHAZOS=f"DELTA_RECHAZOS_{params.sr_folio}",
    DELTA_PRO_DEV_PAG_SJL = f"DELTA_TRANSFORMADA_{params.sr_folio}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

DELTA_TABLE_CONTEOS = f"DELTA_CONTEOS_{params.sr_folio}"
db.write_delta(DELTA_TABLE_CONTEOS,db.sql_delta(statement), "overwrite")

# COMMAND ----------

if conf.debug:
    display(db.read_delta(DELTA_TABLE_CONTEOS))
