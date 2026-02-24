# Databricks notebook source
'''
Descripcion:
    Creación de archivo CTINDI para Devolución de Pago sin Justificación Legal | Esta notebook corresponde a la estracción de los jobs:  JP_PATRIF_GAC_0100_GEN_ARCH_CTINDI_VIV
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      3832 - Devolución de Pago sin Justificación Legal

Tablas INPUT:
    PROCESOS.TTCRXGRAL_DEV_PAG_SJL
    CIERREN.TTAFOGRAL_MOV_VIV
    CIERREN.TMSISGRAL_MAP_NCI_ITGY
    CIERREN.TCCRXGRAL_TIPO_SUBCTA
    CIERREN.TCAFOGRAL_VALOR_ACCION
Tablas OUTPUT:
    CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO
Tablas INPUT DELTA:
    N/A
Tablas OUTPUT DELTA:
    N/A
Archivos SQL:
    DELETE.sql
    CTINDI_DPSJL_TRN_0300_DBK.sql
'''

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_proceso": str,
    "sr_subproceso": str,
    "sr_subetapa": str,
    "sr_folio": str,
    #"sr_mask_rec_trp": str,
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

# DBTITLE 1,Extracción base
#se hace el join con el campo 002

statement = query.get_statement(
    "CTINDI_DPSJL_TRN_0300_DBK.sql",
    sr_subproceso = params.sr_subproceso,
    sr_clave_ent_orig = '002',
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_01_{params.sr_folio}",
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_02_{params.sr_folio}"

)

db.write_delta(f"DELTA_CTINDI_07_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_07_{params.sr_folio}"))

# COMMAND ----------

# DBTITLE 1,Primera insersión a BD
#INSERTA TTSISGRAL_ETL_GEN_ARCHIVO

table_name = "CIERREN_ETL.TTSISGRAL_ETL_GEN_ARCHIVO"

borrado = query.get_statement(
    "DELETE.sql",
    SR_FOLIO=params.sr_folio,
    table_name=table_name,
    hints="/*+ PARALLEL(8) */",
)

execution = db.execute_oci_dml(
    statement=borrado, async_mode=False
)

db.write_data(db.read_delta(f"DELTA_CTINDI_07_{params.sr_folio}"), table_name, "default", "append")
