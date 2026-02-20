# Databricks notebook source
# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_aeim": str,
    "sr_dt_org_arc": str,
    "sr_fecha_acc": str,
    "sr_folio": str,
    "sr_folio_rel": str,
    "sr_origen_arc": str,
    "sr_proceso": str,
    "sr_reproceso": str,
    "sr_subetapa": str,
    "sr_subproceso": str,
    "sr_tipo_mov": str,
    "sr_instancia_proceso": str,
    "sr_usuario": str,
    "sr_etapa": str,
    "sr_id_snapshot": str,
})
# Validar widgets
params.validate()

if params.sr_folio_rel.lower() == 'na':
    params.sr_folio_rel = ''

# COMMAND ----------

conf = ConfManager()

#Archivos SQL
query = QueryManager()

#Conexion a base de datos
db = DBXConnectionManager()

# COMMAND ----------

#Query Ext AEIM

statement = query.get_statement(
    "MCV_AEIM_EXT_0100_OCI_AEIM.sql",
    SR_TIPO_MOV=params.sr_tipo_mov,
    SR_FOLIO=params.sr_folio,
    SR_FOLIO_REL=params.sr_folio_rel,
)

db.write_delta(f"DELTA_MCV_01_AEIM_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_MCV_01_AEIM_{params.sr_folio}"))

# COMMAND ----------

#Query Ext ETL_DISP

statement = query.get_statement(
    "MCV_AEIM_EXT_0200_OCI_DISP.sql",
    sr_tipo_mov=params.sr_tipo_mov,
    sr_folio=params.sr_folio,
    sr_folio_rel=params.sr_folio_rel,
    sr_proceso=params.sr_proceso,
    sr_subproceso=params.sr_subproceso,
)

db.write_delta(f"DELTA_MCV_02_DISP_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_MCV_02_DISP_{params.sr_folio}"))

# COMMAND ----------

#Query Ext CONCEP_MOV

statement = query.get_statement(
    "MCV_AEIM_EXT_0300_OCI_CONCEP_MOV.sql",
    sr_tipo_mov=params.sr_tipo_mov,
    sr_subproceso=params.sr_subproceso,
)

db.write_delta(f"DELTA_MCV_03_CONCEP_MOV_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_MCV_03_CONCEP_MOV_{params.sr_folio}"))

# COMMAND ----------

#JOIN AEIM - DISP

statement = query.get_statement(
    "MCV_AEIM_TRN_0100_DBK_AEIM.sql",
   # DELTA_TABLA_NAME1 = "DELTA_MCV_01_AEIM_" + params.sr_folio
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_MCV_01_AEIM_{params.sr_folio}",
   # DELTA_TABLA_NAME2 = "DELTA_MCV_02_DISP_" + params.sr_folio
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_MCV_02_DISP_{params.sr_folio}",

)

db.write_delta(f"DELTA_MCV_04_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_MCV_04_{params.sr_folio}"))

# COMMAND ----------

#JOIN CONCEPT_MOV & SUM

statement = query.get_statement(
    "MCV_AEIM_TRN_0200_DBK_CONCEP_MOV.sql",
   # DELTA_TABLA_NAME1 = "DELTA_MCV_01_AEIM_" + params.sr_folio
    DELTA_TABLA_NAME1 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_MCV_04_{params.sr_folio}",
   # DELTA_TABLA_NAME2 = "DELTA_MCV_02_DISP_" + params.sr_folio
    DELTA_TABLA_NAME2 = f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.DELTA_MCV_03_CONCEP_MOV_{params.sr_folio}",

)

db.write_delta(f"TEMP_DOIMSS_MCV_MONTOS_CLIENTE_{params.sr_folio}", db.sql_delta(statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"TEMP_DOIMSS_MCV_MONTOS_CLIENTE_{params.sr_folio}"))

# COMMAND ----------

if params.sr_tipo_mov.lower() == 'cargo':
    table_name = "CIERREN_DATAUX.TTSISGRAL_ETL_MONTOS_CLIENTE_AUX"
    db.write_data(db.read_delta(f"TEMP_DOIMSS_MCV_MONTOS_CLIENTE_{params.sr_folio}"), table_name, "default", "append")
