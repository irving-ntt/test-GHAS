# Databricks notebook source
'''
Descripcion:
    Extracción de información OCI para la creación de archivo CTINDI para 364,365 y368. | Esta notebook corresponde a los Jobs: JP_PATRIF_0010_EXT_TRANSF y JP_PATRIF_GAC_0100_GEN_ARCH_CTINDI 
Subetapa:
    GENERACIÓN DE ARCHIVO CTINDI
Trámite:
      364 - Transferencias de Acreditados Infonavit
      365 - Transferencia por Anualidad Garantizada
      368 - Uso de Garantía por 43 BIS
Tablas INPUT:
    PROCESOS.TTCRXGRAL_TRANS_INFONA
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
    CTINDI_TRANSF_EXT_0100_OCI_MOV_VIV.sql
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
    "CTINDI_TRANSF_EXT_0100_OCI_MOV_VIV.sql",
    sr_folio=params.sr_folio,
    sr_subproceso = params.sr_subproceso,
)

db.write_delta(f"DELTA_CTINDI_01_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_CTINDI_01_{params.sr_folio}"))

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

db.write_data(db.read_delta(f"DELTA_CTINDI_01_{params.sr_folio}"), table_name, "default", "append")
