# Databricks notebook source
'''
Descripcion:
    Proceso que realiza el proceso previo a la inserción de las tablas finales.
Subetapa:
    26 - Cifras Control
Trámite:
    364 - Transferencias de Acreditados Infonavit
    365 - Transferencias por Anualidad Garantizada
    368 - Uso de Garantía por 43 BIS
Tablas input:
    PROCESOS.TTCRXGRAL_TRANS_INFONA
    CIERREN.TRAFOGRAL_MOV_SUBCTA
Tablas output:
    N/A
Tablas delta:
    DELTA_PROC_CONTEO_{params.sr_id_archivo}
    DELTA_MOV_SUBCTA_{params.sr_id_archivo}
    DELTA_TRANS_200_TI_JOIN_{params.sr_id_archivo}
Archivos SQL:
    100_200_SUFSALDOS_TRANS_INF.sql
    100_200_SUFSALDOS_TRANS_INF_400_SUBCTA.sql
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

statement = query.get_statement(
    "100_PROC_CONTEO.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso
)

db.write_delta(f"DELTA_PROC_CONTEO_{params.sr_id_archivo}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_PROC_CONTEO_{params.sr_id_archivo}"))

# COMMAND ----------

statement = f"""
    SELECT FRN_ID_MOV_SUBCTA,FCN_ID_TIPO_SUBCTA
    FROM CIERREN.TRAFOGRAL_MOV_SUBCTA
    WHERE FCN_ID_SUBPROCESO = {params.sr_subproceso}"""

db.write_delta(f"DELTA_MOV_SUBCTA_{params.sr_id_archivo}",db.read_data("default", statement), "overwrite")

# COMMAND ----------

statement = query.get_statement(
    "TRANS_200_TI_JOIN.sql",
    DELTA_600_TRANS= f"DELTA_TI_DS_700_{params.sr_id_archivo}",
    DELTA_PROC_CONTEO=f"DELTA_PROC_CONTEO_{params.sr_id_archivo}",
    DELTA_MOV_SUBCTA=f"DELTA_MOV_SUBCTA_{params.sr_id_archivo}",
    SR_SUBPROCESO=params.sr_subproceso
)

# COMMAND ----------

db.write_delta(f"DELTA_TRANS_200_TI_JOIN_{params.sr_id_archivo}", db.sql_delta(statement),"overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRANS_200_TI_JOIN_{params.sr_id_archivo}"))
