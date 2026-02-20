# Databricks notebook source
"""
Descripcion:
    Lee la información directamente de OCI, realiza las transformaciones necesarias y guarda la información intermedia en tablas delta, finalmente inserta en OCI el resultado
Subetapa: 
    25 - Matriz de Convivnecia
Trámite:
    354 - Solicitud de marca de cuentas por 43 bis
Tablas input:
    PROCESOS.TTCRXGRAL_MARCA_DESMARCA_INFO
    PROCESOS.TTSISGRAL_SUF_SALDOS
    CIERREN.TCAFOGRAL_VALOR_ACCION
    CIERREN.TRAFOGRAL_MOV_SUBCTA
    CIERREN.TFAFOGRAL_CONFIG_SUBPROCESO
Tablas output:
    CIERREN_ETL.TTSISGRAL_ETL_PRE_MATRIZ
Tablas Delta:
    DELTA_TRA_MCV_SOL_MCA_01_{params.sr_folio}
    
Archivos SQL:
    TRA_MCV_SOL_MARCA_0100_EXT_INFO.sql
    TRA_MCV_SOL_MARCA_0200_DEL_PRE_MATRIZ.sql
  

"""

# COMMAND ----------

# MAGIC %run "./startup"

# COMMAND ----------

params = WidgetParams({
    "sr_folio": str,
    "sr_proceso": str,
    "sr_reproceso": str,
    "sr_subetapa": str,     
    "sr_subproceso": str,
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
    "TRA_DB_ATRIF_VSS_0100_FOV_TRANSFER_363_REP.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_FOV_TRANSFER_363_REP_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_FOV_TRANSFER_363_REP_{params.sr_folio}")) 

# COMMAND ----------

DS_600_VAL_CUENTAS = "DELTA_TRA_DB_ATRIF_VSS_0100_FOV_TRANSFER_363_REP_" + params.sr_folio


# COMMAND ----------

#query para DS_700_SALDOS_PROC :
DELTA_TABLE_DS_700_SALDOS_PROC_363 = "DELTA_TRA_DB_ATRIF_VSS_0100_FOV_TRANSFER_363_REP_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_363_DS_700_SALDOS_PROC.sql",
 DELTA_TABLE_DS_700_SALDOS_PROC_363=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DS_700_SALDOS_PROC_363}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DB_ATRIF_DS_700_SALDOS_PROC_FOV_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_DS_700_SALDOS_PROC_FOV_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN SUFICENCIA DE SALDOS AUX
statement = query.get_statement(
    "TRA_DB_DEL_SUF_SALDOS_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN SUFICENCIA DE TTAFOTRAS_TRANS_FOVISSSTE  AUX
statement = query.get_statement(
    "TRA_DB_DEL_TRANS_FOVISSSTE_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------


#query de INSERT DE SUF SALDOS  :
#DELTA_TABLE_INSERT = "DELTA_TRA_DELTA_UNION_SALDOS_" + params.sr_folio
DELTA_TABLE_DB_700_SUF_SALDOS_363 = "DELTA_TRA_DB_ATRIF_VSS_0100_FOV_TRANSFER_363_REP_" + params.sr_folio


statement_004 = query.get_statement(
    "TRA_DB_ATRIF_363_BD_700_SUF_SALDOS.sql",
 DELTA_TABLE_DB_700_SUF_SALDOS_363=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DB_700_SUF_SALDOS_363}",
    sr_folio=params.sr_folio,

)
DLTstatement_004 = df = db.sql_delta(query=statement_004)

db.write_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_363_REP_{params.sr_folio}", DLTstatement_004, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_363_REP_{params.sr_folio}"))

# COMMAND ----------

#INSERTA EN PRE SUF_SALDOS

table_name = "CIERREN_DATAUX.TTSISGRAL_SUF_SALDOS_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_363_REP_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

statement = query.get_statement(
    "TRA_DB_ATRIF_363_BD_700_SUF_SALDOS_MERGE.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

#query para DS_700_SALDOS_PROC :
DELTA_TABLE_BD_600_TRANS_FOV_363 = "DELTA_TRA_DB_ATRIF_VSS_0100_FOV_TRANSFER_363_REP_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_363_BD_600_TRANS_FOV.sql",
 DELTA_TABLE_BD_600_TRANS_FOV_363=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_BD_600_TRANS_FOV_363}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DB_ATRIF_363_BD_600_TRANS_FOV_REP_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_363_BD_600_TRANS_FOV_REP_{params.sr_folio}"))

# COMMAND ----------

table_name = "CIERREN_DATAUX.TTAFOTRAS_TRANS_FOVISSSTE_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_DB_ATRIF_363_BD_600_TRANS_FOV_REP_{params.sr_folio}"), table_name, "default", "append")


# COMMAND ----------

statement = query.get_statement(
    "TRA_DB_ATRIF_363_BD_600_TRANS_FOV_MERGE.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)
