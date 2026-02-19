# Databricks notebook source
'''
Descripcion:
    Creación de archivo CTINDI para Devolución de Excedentes por 43 BIS y Devolución de Saldos Excedentes por T.A.A.G..  | Esta notebook corresponde a la estracción de los jobs: JP_PATRIF_GAC_0100_EXT_DEV_SLDS, JP_PATRIF_GAC_0100_GEN_ARCH_CTINDI
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      348 - Devolución de Excedentes por 43 BIS
      349 - Devolución de Saldos Excedentes por T.A.A.G.
Tablas INPUT:
    PROCESOS.TTAFOTRAS_DEVO_SALDO_EXC_INFO
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
    CTINDI_DEX_43_BIS_EXT_0100_OCI_MOV_VIV.sql
    CTINDI_DEX_43_BIS_TRN_0200_DBK.sql
    DELETE.sql
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

#Query DITIMSS

statement = query.get_statement(
    "CTINDI_DEX_43_BIS_EXT_0100_OCI_MOV_VIV.sql",
    sr_folio=params.sr_folio,
    sr_subproceso = params.sr_subproceso,
)

db.write_delta(f"DELTA_CTINDI_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_01_{params.sr_folio}"))

# COMMAND ----------



statement = query.get_statement(
    "CTINDI_DEX_43_BIS_TRN_0200_DBK.sql",
    sr_subproceso = params.sr_subproceso,
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_CTINDI_01_{params.sr_folio}"

)

db.write_delta(f"DELTA_CTINDI_02_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_02_{params.sr_folio}"))

# COMMAND ----------

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

db.write_data(db.read_delta(f"DELTA_CTINDI_02_{params.sr_folio}"), table_name, "default", "append")
