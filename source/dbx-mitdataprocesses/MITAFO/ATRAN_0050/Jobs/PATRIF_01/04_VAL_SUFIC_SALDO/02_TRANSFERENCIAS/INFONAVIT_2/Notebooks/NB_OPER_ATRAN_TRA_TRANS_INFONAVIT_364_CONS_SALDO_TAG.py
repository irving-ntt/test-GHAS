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
    "TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_600_VAL_CUENTAS.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_600_VAL_CUENTAS_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_600_VAL_CUENTAS_{params.sr_folio}")) 

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_700_REJ_CUENTAS.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_700_REJ_CUENTAS_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_700_REJ_CUENTAS_{params.sr_folio}"))

# COMMAND ----------

#Query Extrae informacion para pre matriz

statement = query.get_statement(
    "TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_400_RejInsSaldos.sql",
    sr_folio=params.sr_folio,
)

db.write_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_400_RejInsSaldos_{params.sr_folio}", db.read_data("default", statement), "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_400_RejInsSaldos_{params.sr_folio}"))

# COMMAND ----------

DS_600_VAL_CUENTAS = "DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_600_VAL_CUENTAS_" + params.sr_folio
DS_400_RejInsSaldos = "DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_400_RejInsSaldos_" + params.sr_folio
DS_700_REJ_CUENTAS = "DELTA_TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_DS_700_REJ_CUENTAS_" + params.sr_folio

# COMMAND ----------

statement_001 = query.get_statement(
"TRA_DB_ATRIF_VSS_0100_INFONAVIT_TRANSFER_364_UNION.sql",
 DS_600_VAL_CUENTAS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_600_VAL_CUENTAS}",
 DS_400_RejInsSaldos=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_400_RejInsSaldos}",
 DS_700_REJ_CUENTAS=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DS_700_REJ_CUENTAS}",
)

DLTstatement_001 = df = db.sql_delta(query=statement_001)

db.write_delta(f"DELTA_TRA_INFONAVIT_TRANSFER_364_UNION_{params.sr_folio}", DLTstatement_001, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_INFONAVIT_TRANSFER_364_UNION_{params.sr_folio}"))

# COMMAND ----------

#query para DS_700_SALDOS_PROC :
DELTA_TABLE_DS_700_SALDOS_PROC_364 = "DELTA_TRA_INFONAVIT_TRANSFER_364_UNION_" + params.sr_folio


statement_003 = query.get_statement(
    "TRA_DB_ATRIF_364_DS_700_SALDOS_PROC.sql",
 DELTA_TABLE_DS_700_SALDOS_PROC_364=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DS_700_SALDOS_PROC_364}",
     sr_folio=params.sr_folio,
)
DLTstatement_003 = df = db.sql_delta(query=statement_003)

db.write_delta(f"DELTA_TRA_DB_ATRIF_DS_700_SALDOS_PROC_{params.sr_folio}", DLTstatement_003, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DB_ATRIF_DS_700_SALDOS_PROC_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN SUFICENCIA DE SALDOS
statement = query.get_statement(
    "TRA_DB_DEL_SUF_SALDOS.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------


#query de INSERT DE SUF SALDOS  :
#DELTA_TABLE_INSERT = "DELTA_TRA_DELTA_UNION_SALDOS_" + params.sr_folio
DELTA_TABLE_DS_900_SUF_SALDOS_PROC_364 = "DELTA_TRA_INFONAVIT_TRANSFER_364_UNION_" + params.sr_folio


statement_004 = query.get_statement(
    "TRA_DB_ATRIF_364_DS_900_SUF_SALDOS_PROC.sql",
 DELTA_TABLE_DS_900_SUF_SALDOS_PROC_364=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DS_900_SUF_SALDOS_PROC_364}",
    sr_folio=params.sr_folio,

)
DLTstatement_004 = df = db.sql_delta(query=statement_004)

db.write_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_364_{params.sr_folio}", DLTstatement_004, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_364_{params.sr_folio}"))

# COMMAND ----------

#INSERTA EN PRE SUF_SALDOS

table_name = "PROCESOS.TTSISGRAL_SUF_SALDOS"

db.write_data(db.read_delta(f"DELTA_TRA_DELTA_INSERT_SALDOS_364_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

#query de INSERT DE TRANS_INFONA :

DELTA_TABLE_DS_900_SUF_SALDOS_PROC_364 = "DELTA_TRA_INFONAVIT_TRANSFER_364_UNION_" + params.sr_folio


statement_004 = query.get_statement(
    "TRA_DB_ATRIF_364_BD_800_TRANS_INFONA.sql",
 DELTA_TABLE_DS_900_SUF_SALDOS_PROC_364=f"{SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{DELTA_TABLE_DS_900_SUF_SALDOS_PROC_364}",
    sr_folio=params.sr_folio,

)
DLTstatement_004 = df = db.sql_delta(query=statement_004)

db.write_delta(f"DELTA_TRA_DELTA_TRANS_INFONA_364_{params.sr_folio}", DLTstatement_004, "overwrite")

if conf.debug:
    display(db.read_delta(f"DELTA_TRA_DELTA_TRANS_INFONA_364_{params.sr_folio}"))

# COMMAND ----------

# ELIMINA REGISTROS DEL FOLIO BITACORA EN TRANS_INFONA_AUX
statement = query.get_statement(
    "TRA_DB_DEL_TRANS_INFONA_AUX.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)

# COMMAND ----------

table_name = "CIERREN_DATAUX.TTCRXGRAL_TRANS_INFONA_AUX"

db.write_data(db.read_delta(f"DELTA_TRA_DELTA_TRANS_INFONA_364_{params.sr_folio}"), table_name, "default", "append")

# COMMAND ----------

statement = query.get_statement(
    "TRA_DB_ATRIF_364_MERGE_TRANS_INFONA.sql",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(8) */",
)
execution = db.execute_oci_dml(
    statement=statement, async_mode=False
)
