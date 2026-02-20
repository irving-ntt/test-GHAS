# Databricks notebook source
'''
Descripcion:
    Proceso que realiza para la separación de subcuenta (15/16 INFONAVIT).
Subetapa:
    26 - Cifras Control
Trámite:
    364 - Transferencias de Acreditados Infonavit
    365 - Transferencias por Anualidad Garantizada
    368 - Uso de Garantía por 43 BIS
Tablas input:
    PROCESOS.TTSISGRAL_SUF_SALDOS
    PROCESOS.TTCRXGRAL_TRANS_INFONA
Tablas output:
    N/A
Tablas delta:
    DELTA_400_CTA_INDV_{params.sr_folio}
    DELTA_700_ACT_{params.sr_folio}
Archivos SQL:
    100_200_SUFSALDOS_TRANS_INF_ACT.sql
    100_200_SUFSALDOS_TRANS_INF_ACT_92_97.sql
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

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "100_200_SUFSALDOS_TRANS_INF_ACT.sql",
    SR_FOLIO=params.sr_folio,
    SR_SUBPROCESO=params.sr_subproceso,
)

db.write_delta(f"DELTA_400_CTA_INDV_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_400_CTA_INDV_{params.sr_folio}"))

# COMMAND ----------

statement = query.get_statement(
    "100_200_SUFSALDOS_TRANS_INF_ACT_92_97.sql",
    DELTA_400_CTA_INDV=f"DELTA_400_CTA_INDV_{params.sr_folio}",
)

db.write_delta(f"DELTA_700_ACT_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_700_ACT_{params.sr_folio}"))
