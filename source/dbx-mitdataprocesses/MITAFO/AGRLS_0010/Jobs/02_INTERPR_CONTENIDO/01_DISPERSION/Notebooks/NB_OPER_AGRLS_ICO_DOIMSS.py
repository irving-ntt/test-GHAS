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
    "INCO_DOIMSS_EXT_100_OCI_LEE_ARCHIVO.sql",
    sr_id_archivo=params.sr_id_archivo,
)

db.write_delta(f"DELTA_INCO_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

# COMMAND ----------

#Query DITIMSS

statement = query.get_statement(
    "INCO_GRL_EXT_0200_OCI_LEE_ARCHIVO.sql",
    sr_id_archivo=params.sr_id_archivo,
)

db.write_delta(f"DELTA_INCO_02_{params.sr_folio}", db.read_data("default", statement), "overwrite")

# COMMAND ----------

#Query DITIMSS

statement = query.get_statement(
    "INCO_GRL_EXT_0300_OCI_CAT_SUBCTA.sql",
    sr_id_archivo=params.sr_id_archivo,
)

db.write_delta(f"DELTA_INCO_03_{params.sr_folio}", db.read_data("default", statement), "overwrite")

# COMMAND ----------

#Query TF --> Mapeo Pre Dispersión

statement = query.get_statement(
    "INCO_DOIMSS_TRN_400_DBK_PRE_DIS.sql",
    sr_folio=params.sr_folio,
    sr_proceso = params.sr_proceso,
    sr_subproceso = params.sr_subproceso,
   # DELTA_TABLA_NAME1 = "DELTA_INCO_01_" + params.sr_folio
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_01_{params.sr_folio}"

)

db.write_delta(f"DELTA_INCO_04_{params.sr_folio}", db.sql_delta(statement), "overwrite")

# COMMAND ----------

#Query TF --> Mapeo CLIENTE SUA

statement = query.get_statement(
    "INCO_DOIMSS_TRN_500_DBK_CTE_SUA.sql",
    sr_folio=params.sr_folio,
    sr_proceso = params.sr_proceso,
    sr_subproceso = params.sr_subproceso,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_01_{params.sr_folio}"

)

db.write_delta(f"DELTA_INCO_05_{params.sr_folio}", db.sql_delta(statement), "overwrite")

# COMMAND ----------

#Query Join --> CLIENTE SUA

statement = query.get_statement(
    "INCO_GRL_TRN_0600_DBK_PRE_DIS.sql",
    DELTA_TABLA_NAME5 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_05_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_02_{params.sr_folio}"

)

#El contenido de esta tabla delta se va a insertar en CLIENTE SUA
db.write_delta(f"DELTA_INCO_06_{params.sr_folio}", db.sql_delta(statement), "overwrite")


# COMMAND ----------

#Query Join --> PRE DISPERSION

statement = query.get_statement(
    "INCO_GRL_TRN_0700_DBK_PRE_DIS.sql",
    DELTA_TABLA_NAME4 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_04_{params.sr_folio}",
    DELTA_TABLA_NAME3 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_03_{params.sr_folio}"

)

#El contenido de esta tabla delta se va a insertar en PRE DISPERSION
db.write_delta(f"DELTA_INCO_07_{params.sr_folio}", db.sql_delta(statement), "overwrite")