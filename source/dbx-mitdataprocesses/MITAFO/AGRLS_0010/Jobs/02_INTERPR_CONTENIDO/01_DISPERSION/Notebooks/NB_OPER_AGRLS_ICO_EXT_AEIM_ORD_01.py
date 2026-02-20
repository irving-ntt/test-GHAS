# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_id_archivo": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
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

#Query DITIMSS

statement = query.get_statement(
    "INCO_AEIM_EXT_100_OCI_ORD_LEE_ARCHIVO.sql",
    sr_id_archivo=params.sr_id_archivo,
)

db.write_delta(f"DELTA_INCO_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_INCO_01_{params.sr_folio}"))

# COMMAND ----------

#Query DITIMSS

statement = query.get_statement(
    "INCO_GRL_EXT_0200_OCI_LEE_ARCHIVO.sql",
    sr_id_archivo=params.sr_id_archivo,
)

db.write_delta(f"DELTA_INCO_02_{params.sr_folio}", db.read_data("default", statement), "overwrite")
if conf.debug:
    display(db.read_delta(f"DELTA_INCO_02_{params.sr_folio}"))

# COMMAND ----------

#Query DITIMSS

statement = query.get_statement(
    "INCO_GRL_EXT_0300_OCI_CAT_SUBCTA.sql",
    sr_id_archivo=params.sr_id_archivo,
)

db.write_delta(f"DELTA_INCO_03_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_INCO_03_{params.sr_folio}"))
