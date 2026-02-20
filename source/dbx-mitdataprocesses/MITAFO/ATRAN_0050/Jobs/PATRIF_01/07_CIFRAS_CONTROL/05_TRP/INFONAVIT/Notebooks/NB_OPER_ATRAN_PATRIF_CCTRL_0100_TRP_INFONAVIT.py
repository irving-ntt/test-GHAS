# Databricks notebook source
"""
Descripcion:
    Se realiza la separación de subsecuenta de transferencias de recursos de portabilidad
Subetapa: 
    26 - Cifras Control
Trámite:
    3286 - Transferencia de Recursos por portabilidad
Tablas input:
    PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
    PROCESOS.TTSISGRAL_SUF_SALDOS
Tablas output:
    N/A
Tablas Delta:
    DELTA_300_JOIN_CTA_INDV_{params.sr_folio}
    DELTA_600_TRANS_{params.sr_folio}
Archivos SQL:
    300_JOIN_CTA_INDV.sql
    FN_600_REG.sql
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
    "sr_tipo_layout":str,
})
# Validar widgets
# params.validate()

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

statement = query.get_statement(
    "300_JOIN_CTA_INDV.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

df = db.read_data("default",statement)

DELTA_TABLE_001 = f"DELTA_300_JOIN_CTA_INDV_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001,df , "overwrite")

# COMMAND ----------

if conf.debug:
    display(df)

# COMMAND ----------

DELTA_TABLE_600 = f"DELTA_600_TRANS_{params.sr_folio}"

statement = query.get_statement(
    "FN_600_REG.sql",
    DELTA_300_JOIN_CTA_INDV = f"DELTA_300_JOIN_CTA_INDV_{params.sr_folio}",
)

df_600 = db.sql_delta(query=statement)
db.write_delta(DELTA_TABLE_600, df_600, "overwrite")

# COMMAND ----------

if conf.debug:
    display(df_600)
