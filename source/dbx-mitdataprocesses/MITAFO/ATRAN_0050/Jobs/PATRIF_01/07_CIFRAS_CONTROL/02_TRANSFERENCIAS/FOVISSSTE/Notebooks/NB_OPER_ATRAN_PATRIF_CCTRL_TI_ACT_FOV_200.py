# Databricks notebook source
'''
Descripcion:
    Proceso que realiza el proceso previo a la inserción de las tablas finales.
Subetapa:
    26 - Cifras Control
Trámite:
    363 - Transferencia de acreditados FOVISSSTE
Tablas input:
    CIERREN.TRAFOGRAL_MOV_SUBCTA
Tablas output:
    N/A
Tablas delta:
    DELTA_PROC_CONTEO_{params.sr_id_archivo}
    DELTA_MOV_SUBCTA_{params.sr_id_archivo}
    DELTA_TRANS_200_TI_JOIN_{params.sr_id_archivo}
Archivos SQL:
    100_200_SUFSALDOS_TRANS_INF.sql
'''

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

statement = f"""
    SELECT FRN_ID_MOV_SUBCTA,FCN_ID_TIPO_SUBCTA
    FROM CIERREN.TRAFOGRAL_MOV_SUBCTA
    WHERE FCN_ID_SUBPROCESO = {params.sr_subproceso}"""

db.write_delta(f"DELTA_MOV_SUBCTA_{params.sr_id_archivo}",db.read_data("default", statement), "overwrite")

# COMMAND ----------

statement = query.get_statement(
    "200_RECH_TRANS_FOV.sql",
    DELTA_600_TRANS= f"DELTA_700_ACT_{params.sr_folio}",
    DELTA_MOV_SUBCTA=f"DELTA_MOV_SUBCTA_{params.sr_id_archivo}",
)

db.write_delta(f"DELTA_TRANS_400_JOIN_{params.sr_id_archivo}", db.sql_delta(statement),"overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRANS_400_JOIN_{params.sr_id_archivo}"))

# COMMAND ----------


