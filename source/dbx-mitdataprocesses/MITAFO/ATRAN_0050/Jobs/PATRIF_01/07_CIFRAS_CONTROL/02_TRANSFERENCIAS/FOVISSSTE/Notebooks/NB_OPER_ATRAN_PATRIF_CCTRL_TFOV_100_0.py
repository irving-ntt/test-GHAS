# Databricks notebook source
'''
Descripcion:
    Proceso que realiza el proceso de separación de subcuenta 17 y 18.
Subetapa:
    26 - Cifras Control
Trámite:
    364 - Transferencias de Acreditados Fovissste
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
    "100_200_SUFSALDOS_TRANS_INF.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

db.write_delta(f"DELTA_300_CTA_IND_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_300_CTA_IND_{params.sr_folio}"))

# COMMAND ----------

DELTA_TABLE_600 = f"DELTA_TI_DS_700_{params.sr_folio}"

statement = query.get_statement(
    "FL_400_SUBCTA_TRANS_FOV.sql",
    DELTA_FL_400_SUBCTA = f"DELTA_300_CTA_IND_{params.sr_folio}"
)

df_600 = db.sql_delta(query=statement)
db.write_delta(DELTA_TABLE_600, df_600, "overwrite")
