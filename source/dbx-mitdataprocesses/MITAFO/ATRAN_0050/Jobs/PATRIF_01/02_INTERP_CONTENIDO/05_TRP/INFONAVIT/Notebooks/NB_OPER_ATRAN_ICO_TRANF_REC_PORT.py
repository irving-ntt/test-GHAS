# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y carga la información directo en las tablas de OCI
Subetapa: 
    483 - Interpretación de Contenido
Trámite:
    3286 - Transferencia de Recursos por Portabilidad
Tablas input:
    CIERREN_ETL.TTSISGRAL_ETL_LEE_ARCHIVO
Tablas output:
    CIERREN_ETL.TTAFOTRAS_ETL_TRANS_REC_PORTA
Tablas Delta:
    DELTA_INCO_01_{params.sr_folio}
    DELTA_INCO_02_{params.sr_folio}
    
Archivos SQL:
    INCO_TRANF_REC_PORT_EXT_0100_OCI_LEE_ARCHIVO
    INCO_TRANF_REC_PORT_TRN_0200_DBK_TRANSF_INFONAVIT
    DELETE
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
    #parametros nuevos
    
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

#Query Extraccion de información de OCI

statement = query.get_statement(
    "INCO_TRANF_REC_PORT_EXT_0100_OCI_LEE_ARCHIVO.sql",
    sr_id_archivo=params.sr_id_archivo,
)



db.write_delta(f"DELTA_INCO_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_INCO_01_{params.sr_folio}"))

# COMMAND ----------

#Query Transformación de información

statement = query.get_statement(
    "INCO_TRANF_REC_PORT_TRN_0200_DBK_TRANSF_INFONAVIT.sql",
    sr_folio=params.sr_folio,
    sr_id_archivo=params.sr_id_archivo,
    sr_tipo_archivo=f'{params.sr_tipo_archivo}',
    sr_mask_rec_trp=f'{params.sr_mask_rec_trp}',
    SR_SUBPROCESO=f'{params.sr_subproceso}',
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_INCO_01_{params.sr_folio}",
)


db.write_delta(f"DELTA_INCO_02_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_INCO_02_{params.sr_folio}"))

# COMMAND ----------

#INSERTA MARCA DESMARCA

table_name = "CIERREN_ETL.TTAFOTRAS_ETL_TRANS_REC_PORTA"

borrado = query.get_statement(
    "DELETE.sql",
    SR_FOLIO=params.sr_folio,
    table_name=table_name,
    hints="/*+ PARALLEL(8) */",
)

execution = db.execute_oci_dml(
    statement=borrado, async_mode=False
)

db.write_data(db.read_delta(f"DELTA_INCO_02_{params.sr_folio}"), table_name, "default", "append")
