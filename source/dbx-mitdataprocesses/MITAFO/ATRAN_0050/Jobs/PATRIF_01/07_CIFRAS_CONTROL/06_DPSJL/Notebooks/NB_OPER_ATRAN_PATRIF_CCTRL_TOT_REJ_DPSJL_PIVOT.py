# Databricks notebook source
"""
Descripcion:
    Se realiza el proceso de pivoteo en la tabla para generar tabla delta DS_900
Subetapa: 
    26 - Cifras Control
Trámite:
    3832 - Devolución de pago sin Justificación Legal
Tablas input:
    N/A
Tablas output:
    N/A   
Tablas Delta:
    DELTA_900_CIF_RECH_{params.sr_folio} 
Archivos SQL:
    100_TOT_REJ_DPSJL_PIVOT.sql
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

statement = query.get_statement(
    "100_TOT_REJ_DPSJL_PIVOT.sql",
    # DELTA_600_GEN_MOV=f"DELTA_600_GEN_MOV_{params.sr_folio}",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)
DELTA_TABLE_001 = f"DELTA_900_CIF_RECH_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001, db.read_data("default",statement), "overwrite")

# COMMAND ----------

if conf.debug:
    display(db.read_delta(DELTA_TABLE_001))
