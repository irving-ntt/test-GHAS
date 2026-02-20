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
    PROCESOS.TTCRXGRAL_VAL_SDOS_DPSJL
Tablas output:
    N/A
Tablas Delta:
    DELTA_VAL_SDOS_DPSJL_{params.sr_folio}
    DELTA_RECHAZOS_{params.sr_folio}
Archivos SQL:
    100_VAL_SDOS_DPSJL.sql
    200_TOT_DPSJL_JOIN_RECHAZOS.sql
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

statement_val_sdos = query.get_statement(
    "100_VAL_SDOS_DPSJL.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

DELTA_TABLE_001 = f"DELTA_VAL_SDOS_DPSJL_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001,db.read_data("default",statement_val_sdos) , "overwrite")

# COMMAND ----------

if conf.debug:
    display(db.read_delta(DELTA_TABLE_001))

# COMMAND ----------

statement = query.get_statement(
    "200_TOT_DPSJL_JOIN_RECHAZOS.sql",
    DELTA_VAL_SDOS = DELTA_TABLE_001,
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
    DELTA_900 = f"DELTA_900_CIF_RECH_{params.sr_folio}",
)

DELTA_TABLE_002 = f"DELTA_RECHAZOS_{params.sr_folio}"
db.write_delta(DELTA_TABLE_002, db.sql_delta(statement), "overwrite")

# COMMAND ----------

if conf.debug:
    display(db.sql_delta(statement))
