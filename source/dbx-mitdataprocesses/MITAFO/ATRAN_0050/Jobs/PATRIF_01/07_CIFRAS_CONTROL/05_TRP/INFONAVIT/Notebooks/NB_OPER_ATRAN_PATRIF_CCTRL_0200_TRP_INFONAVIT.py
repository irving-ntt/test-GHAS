# Databricks notebook source
"""
Descripcion:
    Job paralelo que realiza las transformaciones antes de insertar a cifras y sumario archivo
Subetapa: 
    26 - Cifras Control
Trámite:
    3286 - Transferencia de Recursos por portabilidad
Tablas input:
    PROCESOS.TTAFOTRAS_TRANS_REC_PORTA
    CIERREN.TRAFOGRAL_MOV_SUBCTA
Tablas output:
    N/A
Tablas Delta:
    DELTA_PROC_CONTEO_{params.sr_folio}
    DELTA_MOV_SUBMARCA_{params.sr_folio}
    DELTA_JO_400_{params.sr_folio}
Archivos SQL:
    100_PROC_CONTEO.sql
    300_MOV_SUBMARCA.sql
    JO_400_INNER.sql
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

statement_proc_conteo = query.get_statement(
    "100_PROC_CONTEO.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

df = db.read_data("default",statement_proc_conteo)

DELTA_TABLE_001 = f"DELTA_PROC_CONTEO_{params.sr_folio}"
db.write_delta(DELTA_TABLE_001,df , "overwrite")

# COMMAND ----------

statement_proc_conteo = query.get_statement(
    "300_MOV_SUBMARCA.sql",
    SR_SUBPROCESO=params.sr_subproceso,
)

df_mov = db.read_data("default",statement_proc_conteo)

DELTA_TABLE_MOV = f"DELTA_MOV_SUBMARCA_{params.sr_folio}"
db.write_delta(DELTA_TABLE_MOV,df_mov , "overwrite")

# COMMAND ----------

if conf.debug:
    display(df)
    display(df_mov)

# COMMAND ----------


DELTA_TABLE_400 = f"DELTA_JO_400_{params.sr_folio}"

statement = query.get_statement(
    "JO_400_INNER.sql",
    DELTA_600 = f"DELTA_600_TRANS_{params.sr_folio}",
    DELTA_PROC_CONTEO = f"DELTA_PROC_CONTEO_{params.sr_folio}",
    DELTA_MOV_SUBMARCA = f"DELTA_MOV_SUBMARCA_{params.sr_folio}",
)

df_400 = db.sql_delta(query=statement)
db.write_delta(DELTA_TABLE_400, df_400, "overwrite")

# COMMAND ----------

if conf.debug:
    display(df_400)
