# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    483 - Interpretación de Contenido
Trámite:
    368 - Solicitud Uso de Garantía
Tablas input:
    CIERREN_ETL.TTSISGRAL_ETL_LEE_ARCHIVO
Tablas output:
    CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_INFONAVIT
Tablas Delta:
    DELTA_INCO_SUG_01_{params.sr_folio}
    DELTA_INCO_SUG_02_{params.sr_folio}

Archivos SQL:
    INCO_TRANF_SUG_EXT_100_OCI_LEE_ARCHIVO.sql
    INCO_TRANF_SUG_TRN_200_DBK_TRANSF_INFONAVIT.sql
    INCO_TRANF_SUG_DEL_300_OCI_TRANSF_INFONAVIT.sql

"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    "sr_tipo_archivo": str,
    "sr_id_archivo": str,
    "sr_mask_rec_trp": str,
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

#Query Ext Solicitud Transferencia de acreditados

statement = query.get_statement(
    "INCO_TRANF_SUG_EXT_100_OCI_LEE_ARCHIVO.sql",
    sr_id_archivo=params.sr_id_archivo,
)

db.write_delta(f"DELTA_INCO_SUG_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_INCO_SUG_01_{params.sr_folio}"))

# COMMAND ----------

#Query TF --> Mapeo TRANSF_INFONAVIT

statement = query.get_statement(
    "INCO_TRANF_SUG_TRN_200_DBK_TRANSF_INFONAVIT.sql",
    sr_folio=params.sr_folio,
    sr_id_archivo= params.sr_id_archivo,
    sr_subproceso = params.sr_subproceso,
    sr_tipo_archivo = params.sr_tipo_archivo,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_SUG_01_{params.sr_folio}",

)

db.write_delta(f"DELTA_INCO_SUG_02_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_INCO_SUG_02_{params.sr_folio}"))

# COMMAND ----------

statement = query.get_statement(
    "DELETE.sql",
    table_name = "CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_INFONAVIT",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

#INSERTA TTAFOTRAS_ETL_TRANSF_INFONAVIT

table_name = "CIERREN_ETL.TTAFOTRAS_ETL_TRANSF_INFONAVIT"

db.write_data(db.read_delta(f"DELTA_INCO_SUG_02_{params.sr_folio}"), table_name, "default", "append")
